defmodule Microlsm.Debug do
  @moduledoc false
  debug? = true

  if debug? do
    @debug_queue_size 30

    def debug?, do: true

    defmacro dlog(msg) do
      quote do: unquote(__MODULE__).do_dlog(unquote(msg))
    end

    defmacro wrap_dlog(do: code) do
      quote do
        try do
          unquote(code)
        rescue
          exception ->
            queue = Process.get(:__debug_queue, {[], []})
            log = Enum.reject(:queue.to_list(queue), &is_nil/1)
            IO.inspect log, label: inspect(self())
            reraise exception, __STACKTRACE__
        end
      end
    end

    def do_dlog(msg) do
      queue =
        with nil <- Process.get(:__debug_queue) do
          :queue.from_list List.duplicate(nil, @debug_queue_size)
        end

      queue = :queue.drop(queue)
      msg = {:erlang.system_time(:millisecond), msg}
      queue = :queue.in(msg, queue)
      Process.put(:__debug_queue, queue)
    end
  else
    def debug?, do: false
    defmacro dlog(_), do: nil
    defmacro wrap_dlog(do: code), do: code
  end
end
