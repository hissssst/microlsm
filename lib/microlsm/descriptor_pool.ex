defmodule Microlsm.DescriptorPool do
  # Round-robin pool with atomics compare_exchange for counter
  # and ets select_replace for locking which lazily opens the file descriptors in
  # isolated cell processes

  # TODO implement a failsafe when caller dies during checkout
  # Use Process.link and some supervisor for cells

  @opaque t :: {term(), :atomics.atomics_ref(), pos_integer(), binary()}

  @opaque pool :: :ets.table()

  bypass? = false

  import Microlsm.Debug

  @spec new() :: pool()
  def new do
    :ets.new(__MODULE__, [:set, :public])
  end

  if bypass? do
    def add(_pool, filename, _count) do
      filename
    end

    def remove(_pool, _state) do
      :ok
    end

    def checkout(_pool, filename, func, _timeout \\ nil) do
      {:ok, fd} = :prim_file.open(filename, [:read])
      try do
        func.(fd)
      after
        :prim_file.close(fd)
      end
    end
  else
    @spec add(pool(), filename :: String.t(), pos_integer()) :: t()
    def add(_pool, filename, count) do
      filename_ref = :erlang.unique_integer()
      atomics_ref = :atomics.new(1, signed: false)

      {filename_ref, atomics_ref, count, filename}
    end

    @spec remove(pool(), t()) :: :ok
    def remove(pool, state) do
      {filename_ref, _atomics_ref, _count, _filename} = state
      pattern = {{filename_ref, :_}, :_, :_}
      :ets.select_delete(pool, [{pattern, [], [true]}])
      :ok
    end

    @spec checkout(pool(), t(), (tuple() -> result), timeout()) :: result
    when result: term()
    def checkout(pool, state, func, timeout \\ 5_000) do
      {filename_ref, atomics_ref, count, filename} = state
      {index, cell} = checkout_cell(pool, filename_ref, filename, atomics_ref, count)
      dlog({:checked_out, filename_ref, index, filename, cell})

      monitor_ref = Process.monitor(cell, alias: :reply_demonitor)

      send(cell, {:execute, monitor_ref, func})
      receive do
        {^monitor_ref, :ok, result} ->
          checkin_cell(pool, filename_ref, index)
          result

        {^monitor_ref, kind, error, stacktrace} ->
          checkin_cell(pool, filename_ref, index)
          :erlang.raise(kind, error, stacktrace)

        after timeout ->
          Process.demonitor(monitor_ref)
          restart_cell(pool, filename_ref, index, filename, cell)
          raise "Timeout"
      end
    end
  end

  ## Cell

  defp loop_cell(fd) do
    message = receive do x -> x end

    case message do
      {:execute, ref, func} ->
        try do
          send(ref, {ref, :ok, func.(fd)})
        catch
          kind, error ->
            send(ref, {ref, kind, error, __STACKTRACE__})
        end
        loop_cell(fd)

      {:stop, ref} ->
        :prim_file.close(fd)
        send(ref, :ok)

      _ ->
        loop_cell(fd)
    end
  end

  ## Checkout

  defp checkin_cell(pool, filename_ref, index) do
    ms = [{{{filename_ref, index}, self(), :"$1"}, [], [{{{{filename_ref, index}}, :free, :"$1"}}]}]
    1 = :ets.select_replace(pool, ms)
  end

  defp restart_cell(pool, filename_ref, index, filename, cell) do
    Process.exit(cell, :kill)
    start_cell(pool, filename_ref, index, filename, :free)
  end

  defp start_cell(pool, filename_ref, index, filename, as \\ self()) do
    ref = make_ref()
    owner = self()

    cell =
      spawn_link(fn ->
        {:ok, fd} = :prim_file.open(filename, [:read])
        ms = [{{{filename_ref, index}, owner, :_}, [], [{{{{filename_ref, index}}, as, self()}}]}]
        1 = :ets.select_replace(pool, ms)
        send(owner, ref)
        loop_cell(fd)
      end)

    receive do
      ^ref -> cell
    end
  end

  defp checkout_cell(pool, filename_ref, filename, atomics_ref, count) do
    value = :atomics.add_get(atomics_ref, 1, 1)
    index = rem(value, count)

    do_checkout_cell(pool, filename_ref, filename, atomics_ref, count, index, index)
  end

  defp do_checkout_cell(pool, filename_ref, filename, atomics_ref, count, start_index, index) do
    owner = self()

    case :ets.insert_new(pool, {{filename_ref, index}, self(), nil}) do
      false ->
        # Ex2ms.fun do {{filename_ref, i}, :free, cell} -> {{filename_ref, i}, self(), cell} end
        ms = [{{{filename_ref, index}, :free, :"$1"}, [], [{{{{filename_ref, index}}, self(), :"$1"}}]}]
        case :ets.select_replace(pool, ms) do
          0 ->
            next_value = :atomics.add_get(atomics_ref, 1, 1)
            next_index = rem(next_value, count)
            dlog(:dp_cnt)
            do_checkout_cell(pool, filename_ref, filename, atomics_ref, count, start_index, next_index)

          1 ->
            [{{^filename_ref, ^index}, ^owner, cell}] = :ets.lookup(pool, {filename_ref, index})
            {index, cell}
        end

      true ->
        cell = start_cell(pool, filename_ref, index, filename)
        dlog(:lazy_descriptor_init)
        {index, cell}
    end
  end
end
