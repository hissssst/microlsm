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

        other ->
          other
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

  # Merges two ordered streams of pairs into a single ordered stream of pairs
  # When both streams have the same key, left one is chosen and right one is dropped
  def merge_streams(lenum, renum) do
    lstream = to_func(lenum)
    rstream = to_func(renum)
    lclos = fn acc -> lstream.(acc, fn elem, _acc -> {:suspend, elem} end) end
    rclos = fn acc -> rstream.(acc, fn elem, _acc -> {:suspend, elem} end) end

    Stream.resource(
      fn -> :first end,
      fn
        :first ->
          case lclos.({:cont, []}) do
            {:suspended, {left_key, left_value}, lclos} ->
              case rclos.({:cont, []}) do
                {:suspended, {right_key, right_value}, rclos} ->
                  cmerge(left_key, left_value, right_key, right_value, lclos, rclos)

                {_, {right_key, right_value}} ->
                  cmerge(left_key, left_value, right_key, right_value, lclos, done_clos())

                {_, []} ->
                  {[{left_key, left_value}], {:dump, lclos}}
              end

            {_, {left_key, left_value}} ->
              case rclos.({:cont, []}) do
                {:suspended, {right_key, right_value}, rclos} ->
                  cmerge(left_key, left_value, right_key, right_value, done_clos(), rclos)

                {_, {right_key, right_value}} ->
                  cmerge(left_key, left_value, right_key, right_value, done_clos(), done_clos())

                {_, []} ->
                  {[{left_key, left_value}], :end}
              end

            {_, []} ->
              {[], {:dump, rclos}}
          end

        {:dump, clos} ->
          case clos.({:cont, []}) do
            {:suspended, {key, value}, clos} ->
              {[{key, value}], {:dump, clos}}

            {_, {key, value}} ->
              {[{key, value}], :end}

            {_, []} ->
              {:halt, :end}
          end

        {:left, left_key, left_value, lclos, rclos} ->
          case rclos.({:cont, []}) do
            {:suspended, {right_key, right_value}, rclos} ->
              cmerge(left_key, left_value, right_key, right_value, lclos, rclos)

            {_, {right_key, right_value}} ->
              cmerge(left_key, left_value, right_key, right_value, lclos, done_clos())

            {_, []} ->
              {[{left_key, left_value}], {:dump, lclos}}
          end

        {:right, right_key, right_value, lclos, rclos} ->
          case lclos.({:cont, []}) do
            {:suspended, {left_key, left_value}, lclos} ->
              cmerge(left_key, left_value, right_key, right_value, lclos, rclos)

            {_, {left_key, left_value}} ->
              cmerge(left_key, left_value, right_key, right_value, done_clos(), rclos)

            {_, []} ->
              {[{right_key, right_value}], {:dump, rclos}}
          end

        :end ->
          {:halt, []}
      end,
      fn _ -> [] end
    )
  end

  defp cmerge(left_key, left_value, right_key, right_value, lclos, rclos)
       when left_key < right_key do
    {[{left_key, left_value}], {:right, right_key, right_value, lclos, rclos}}
  end

  defp cmerge(left_key, left_value, right_key, _right_value, lclos, rclos)
       when left_key == right_key do
    case rclos.({:cont, []}) do
      {:suspended, {right_key, right_value}, rclos} ->
        {[{left_key, left_value}], {:right, right_key, right_value, lclos, rclos}}

      {_, {right_key, right_value}} ->
        {[{left_key, left_value}], {:right, right_key, right_value, lclos, done_clos()}}

      {_, []} ->
        {[{left_key, left_value}], {:dump, lclos}}
    end
  end

  defp cmerge(left_key, left_value, right_key, right_value, lclos, rclos)
       when left_key > right_key do
    {[{right_key, right_value}], {:left, left_key, left_value, lclos, rclos}}
  end

  defp done_clos do
    fn _ -> {:done, []} end
  end
end
