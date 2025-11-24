defmodule Microlsm.Stream do
  @moduledoc false

  @compile {:inline, done_clos: 0}

  def after_hook(enumerable, hook) do
    fn acc, fun ->
      try do
        Enumerable.reduce(enumerable, acc, fun)
      else
        {tag, result} ->
          hook.()
          {tag, result}

        {:suspended, result, cont} ->
          {:suspended, result, do_cont_after_hook(cont, hook)}
      catch
        class, reason ->
          hook.()
          :erlang.raise(class, reason, __STACKTRACE__)
      end
    end
  end

  defp do_cont_after_hook(cont, hook) do
    fn acc ->
      try do
        cont.(acc)
      else
        {tag, result} ->
          hook.()
          {tag, result}

        {:suspended, result, cont} ->
          {:suspended, result, do_cont_after_hook(cont, hook)}
      catch
        class, reason ->
          hook.()
          :erlang.raise(class, reason, __STACKTRACE__)
      end
    end
  end

  def to_func(enumerable) when is_function(enumerable, 2) do
    enumerable
  end

  def to_func(enumerable) do
    fn acc, func -> Enumerable.reduce(enumerable, acc, func) end
  end

  def merge_streams(lenum, renum) do
    lstream = to_func(lenum)
    rstream = to_func(renum)
    lclos = fn acc -> lstream.(acc, fn elem, _acc -> {:suspend, elem} end) end
    rclos = fn acc -> rstream.(acc, fn elem, _acc -> {:suspend, elem} end) end

    fn acc, fun ->
      case lclos.({:cont, []}) do
        {:suspended, {left_key, left_value}, lclos} ->
          case rclos.({:cont, []}) do
            {:suspended, {right_key, right_value}, rclos} ->
              do_merge(left_key, left_value, right_key, right_value, lclos, rclos, acc, fun)

            {_, {right_key, right_value}} ->
              do_merge(left_key, left_value, right_key, right_value, lclos, done_clos(), acc, fun)

            {_, []} ->
              dump(left_key, left_value, lclos, acc, fun)
          end

        {_, {left_key, left_value}} ->
          case rclos.({:cont, []}) do
            {:suspended, {right_key, right_value}, rclos} ->
              do_merge(left_key, left_value, right_key, right_value, done_clos(), rclos, acc, fun)

            {_, {right_key, right_value}} ->
              do_merge(left_key, left_value, right_key, right_value, done_clos(), done_clos(), acc, fun)

            {_, []} ->
              dump(left_key, left_value, done_clos(), acc, fun)
          end

        {_, []} ->
          dump(rclos, acc, fun)
      end
    end
  end

  defp dump(key, value, clos, {:cont, iacc}, fun) do
    acc = fun.({key, value}, iacc)
    dump(clos, acc, fun)
  end

  defp dump(_key, _value, clos, {:halt, iacc}, _fun) do
    clos.({:halt, iacc})
    {:halted, iacc}
  end

  defp dump(key, value, clos, {:suspend, iacc}, fun) do
    cont = fn acc -> dump(key, value, clos, acc, fun) end
    {:suspended, iacc, cont}
  end

  defp dump(clos, {:suspend, iacc}, fun) do
    {:suspended, iacc, fn acc -> dump(clos, acc, fun) end}
  end

  defp dump(clos, {:halt, iacc}, _fun) do
    clos.({:halt, iacc})
    {:halted, iacc}
  end

  defp dump(clos, {:cont, iacc}, fun) do
    case clos.({:cont, []}) do
      {:suspended, {key, value}, clos} ->
        acc = fun.({key, value}, iacc)
        dump(clos, acc, fun)

      {_, {key, value}} ->
        acc = fun.({key, value}, iacc)
        dump(done_clos(), acc, fun)

      {_, []} ->
        {:done, iacc}
    end
  end

  defp do_merge(left_key, left_value, right_key, right_value, lclos, rclos, {:cont, iacc}, fun) do
    case left_key do
      left_key when left_key < right_key ->
        acc = fun.({left_key, left_value}, iacc)
        case lclos.({:cont, []}) do
          {:suspended, {left_key, left_value}, lclos} ->
            do_merge(left_key, left_value, right_key, right_value, lclos, rclos, acc, fun)

          {_done_or_halted, {left_key, left_value}} ->
            do_merge(left_key, left_value, right_key, right_value, done_clos(), rclos, acc, fun)

          {_done_or_halted, []} ->
            dump(right_key, right_value, rclos, acc, fun)
        end

      left_key when left_key == right_key ->
        acc = {:cont, iacc}
        case rclos.({:cont, []}) do
          {:suspended, {right_key, right_value}, rclos} ->
            do_merge(left_key, left_value, right_key, right_value, lclos, rclos, acc, fun)

          {_done_or_halted, {right_key, right_value}} ->
            do_merge(left_key, left_value, right_key, right_value, lclos, done_clos(), acc, fun)

          {_done_or_halted, []} ->
            dump(left_key, left_value, lclos, acc, fun)
        end

      left_key when left_key > right_key ->
        acc = fun.({right_key, right_value}, iacc)
        case rclos.({:cont, []}) do
          {:suspended, {right_key, right_value}, rclos} ->
            do_merge(left_key, left_value, right_key, right_value, lclos, rclos, acc, fun)

          {_done_or_halted, {right_key, right_value}} ->
            do_merge(left_key, left_value, right_key, right_value, lclos, done_clos(), acc, fun)

          {_done_or_halted, []} ->
            dump(left_key, left_value, lclos, acc, fun)
        end
    end
  end

  defp do_merge(_left_key, _left_value, _right_key, _right_value, lclos, rclos, {:halt, iacc}, _fun) do
    lclos.({:halt, iacc})
    rclos.({:halt, iacc})
    {:halted, iacc}
  end

  defp do_merge(left_key, left_value, right_key, right_value, lclos, rclos, {:suspend, iacc}, fun) do
    cont = fn acc -> do_merge(left_key, left_value, right_key, right_value, lclos, rclos, acc, fun) end
    {:suspended, iacc, cont}
  end

  defp done_clos do
    fn _ -> {:done, []} end
  end
end
