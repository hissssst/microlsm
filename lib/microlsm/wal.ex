defmodule Microlsm.Wal do
  import :erlang, only: [term_to_binary: 1, binary_to_term: 1]

  # 10MB
  @batch_read_size 10 * 1024 * 1024

  def push_batch(fd, ops) do
    encoded = for op <- ops, into: "", do: encode_op(op)
    :ok = :prim_file.write(fd, encoded)
    byte_size(encoded)
  end

  def stream(fd) do
    Stream.resource(
      fn -> {0, <<>>} end,
      fn {offset, head} ->
        case :prim_file.pread(fd, offset, @batch_read_size) do
          {:ok, tail} ->
            {ops, leftover, offset, length} = decode_ops(head <> tail, [], offset, 0)
            {[{length, ops}], {offset, leftover}}

          :eof when head == <<>> ->
            {:halt, []}
        end
      end,
      fn _ -> [] end
    )
  end

  defp decode_ops(<<size::64, op::binary-size(size), tail::binary>>, acc, total_size, total_count) do
    decode_ops(tail, [binary_to_term(op) | acc], total_size + size + 8, total_count + 1)
  end

  defp decode_ops(binary, acc, total_size, total_count) do
    {:lists.reverse(acc), binary, total_size + byte_size(binary), total_count}
  end

  defp encode_op(op) do
    encoded = term_to_binary(op)
    <<byte_size(encoded)::64, encoded::binary>>
  end
end
