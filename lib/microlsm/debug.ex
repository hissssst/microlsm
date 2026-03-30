defmodule Microlsm.Debug do
  # Zero-abstraction debugging
  @moduledoc false
  debug? = false

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

    def get_dlog(pid) do
      {_, dict} = Process.info(pid, :dictionary)
      :queue.to_list Keyword.get(dict, :__debug_queue, {[], []})
    end
  else
    def debug?, do: false
    def get_dlog(_), do: []
    defmacro dlog(_), do: nil
    defmacro wrap_dlog(do: code), do: code
  end

  def debug_table(filename) do
    import Microlsm.Disktable
    Microlsm.Fs.init_counters()

    case :prim_file.open(filename, [:read]) do
      {:ok, fd} ->
        with {:ready, _length, _max_block_size, block_offsets_offset} <- IO.inspect(read_header(fd), label: :header) do
          with {_block_count, block_offsets} <- IO.inspect(read_footer(fd, block_offsets_offset, 4096), label: :footer) do
            IO.inspect load(fd), label: :loaded
            IO.inspect read_blocks(fd, block_offsets, block_offsets_offset), label: :blocks, limit: :infinity
            :ok
          end
        end

      other ->
        IO.inspect other, label: :on_open
        :ok
    end
  end

  defp read_blocks(fd, [offset], block_offsets_offset) do
    size = block_offsets_offset - offset
    {:ok, chunk} = :prim_file.pread(fd, offset, size)
    chunk = read_chunk(chunk)
    [{offset, chunk}]
  end

  defp read_blocks(fd, [offset | [next | _] = rest], block_offsets_offset) do
    size = next - offset
    {:ok, chunk} = :prim_file.pread(fd, offset, size)
    chunk = read_chunk(chunk)
    [{offset, chunk} | read_blocks(fd, rest, block_offsets_offset)]
  end

  defp read_chunk(chunk) do
    import :erlang, only: [binary_to_term: 1]
    case chunk do
      <<
        keysize::32,
        valuesize::32,
        encoded_key::binary-size(keysize),
        encoded_value::binary-size(valuesize),
        rest::binary
      >> ->
        pair = {binary_to_term(encoded_key), binary_to_term(encoded_value)}
        [pair | read_chunk(rest)]

      <<>> ->
        []
    end
  end
end
