defmodule Microlsm.Merger do
  import Microlsm.Gentable, only: [disktable: 1]
  import :erlang, only: [element: 2]
  alias Microlsm.Disktable
  alias Microlsm.Afs

  @compile {:inline, pop: 1, next: 1, place: 2, prepare: 1}

  defguardp is_gen(x) when is_integer(x) and x > unquote(- (2 ** 30)) and x < unquote(2 ** 30)

  def stream_merge(disktables) do
    fn acc, fun ->
      states = prepare(disktables)
      do_stream(states, acc, fun)
    end
  end

  defp do_stream(states, {:suspend, iacc}, fun) do
    cont = fn acc -> do_stream(states, acc, fun) end
    {:suspended, iacc, cont}
  end

  defp do_stream(states, {:halt, iacc}, _fun) do
    Enum.each(states, fn {_, _g, _b, {afd, _i, _it, _mit}} -> Afs.close(afd) end)
    {:halted, iacc}
  end

  defp do_stream(states, {:cont, iacc}, fun) do
    case next(states) do
      {item, states} ->
        acc = fun.(item, iacc)
        do_stream(states, acc, fun)

      :done ->
        {:done, iacc}
    end
  end

  defp prepare(inputs) do
    inputs
    |> Enum.map(fn
      {:list, generation, list} ->
        pop({nil, generation, list, :list})

      disktable(filename: filename, index: index, generation: generation) ->
        {:ok, afd} = Afs.open(filename, [:read])
        {_, offset} = element(1, index)
        {_, next_offset} = element(2, index)

        {pread_ref, afd} = Afs.pread(afd, offset, next_offset - offset)
        pop({nil, generation, <<>>, {afd, pread_ref, index, 1, tuple_size(index)}})
    end)
    |> Enum.reject(& &1 == :done)
    |> Enum.sort()
    |> simplify()
    # |> tap(fn s -> IO.inspect Enum.map(s, &element(1, &1)), label: :after end)
  end

  defp simplify([{{k, _}, xgen, _, _} = xs, {{k, _}, ygen, _, _} = ys | rest]) when is_gen(xgen) and is_gen(ygen) and xgen < ygen do
    case pop(ys) do
      :done ->
        simplify([xs | rest])

      ys ->
        rest = place(ys, rest)
        simplify([xs | rest])
    end
  end

  defp simplify([{{k, _}, xgen, _, _} = xs, {{k, _}, ygen, _, _} = ys | rest]) when is_gen(xgen) and is_gen(ygen) and xgen > ygen do
    case pop(xs) do
      :done ->
        simplify([ys | rest])

      xs ->
        rest = place(xs, rest)
        simplify([ys | rest])
    end
  end

  defp simplify([]) do
    []
  end

  defp simplify([head | rest]) do
    [head | simplify(rest)]
  end

  defp next([{head, _, _, _} = state | states]) do
    case pop(state) do
      :done ->
        {head, states}

      state ->
        states = place(state, states)
        {head, states}
    end
  end

  defp next([]) do
    :done
  end

  defp place({{k, _}, xgen, _, _} = xs, [{{k, _}, ygen, _, _} = ys | rest]) when is_gen(xgen) and is_gen(ygen) and xgen < ygen do
    case pop(ys) do
      :done ->
        place(xs, rest)

      ys ->
        rest = place(ys, rest)
        place(xs, rest)
    end
  end

  defp place({{k, _}, xgen, _, _} = xs, [{{k, _}, ygen, _, _} | _] = states) when is_gen(xgen) and is_gen(ygen) and xgen > ygen do
    case pop(xs) do
      :done ->
        states

      xs ->
        place(xs, states)
    end
  end

  defp place({{xk, _}, _, _, _} = xs, [{{yk, _}, _, _, _} = ys | rest]) when xk > yk do
    [ys | place(xs, rest)]
  end

  defp place(xs, rest) do
    [xs | rest]
  end

  defp pop({_item, _generation, <<>>, {afd, _pread_ref, _index, iter, miter}}) when iter > miter do
    # Post-last
    Afs.close(afd)
    :done
  end

  defp pop({_item, generation, <<>>, {afd, kv, index, iter, miter}}) when iter == miter do
    # Last
    {kv, generation, <<>>, {afd, nil, index, iter + 1, miter}}
  end

  defp pop({_item, generation, <<>>, {afd, pread_ref, index, iter, miter}}) when iter == miter - 1 do
    # One by one
    {pread_result, afd} = Afs.receive_pread(afd, pread_ref)

    {_, last_offset} = element(miter, index)
    {kv, afd} = Disktable.aread_kv(afd, last_offset, 4096)
    {:ok, buffer} = pread_result

    {item, buffer} = Disktable.next_in_block(buffer)
    {item, generation, buffer, {afd, kv, index, iter + 1, miter}}
  end

  defp pop({_item, generation, <<>>, {afd, pread_ref, index, iter, miter}}) when iter < miter - 1 do
    # One by one
    {_, offset} = element(iter + 1, index)
    {_, next_offset} = element(iter + 2, index)

    {next_pread_ref, afd} = Afs.pread(afd, offset, next_offset - offset)
    {pread_result, afd} = Afs.receive_pread(afd, pread_ref)

    {:ok, buffer} = pread_result

    {item, buffer} = Disktable.next_in_block(buffer)
    {item, generation, buffer, {afd, next_pread_ref, index, iter + 1, miter}}
  end

  defp pop({_item, generation, buffer, source}) when is_binary(buffer) do
    # Present in buffer
    {item, buffer} = Disktable.next_in_block(buffer)
    {item, generation, buffer, source}
  end

  defp pop({_item, _generation, [], :list}) do
    :done
  end

  defp pop({_item, generation, [head | buffer], source}) do
    # Present in buffer
    {head, generation, buffer, source}
  end
end
