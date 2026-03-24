defmodule Microlsm.Wal do
  @moduledoc false

  import :erlang, only: [term_to_iovec: 1, binary_to_term: 1, iolist_size: 1]
  import Bitwise, only: [<<<: 2]
  alias Microlsm.Fs
  alias Microlsm.SizeException

  # 10MB
  # @batch_read_size 10 * 1024 * 1024
  @batch_read_size 10 * 1024 * 1024

  # Literally infinite size of an operation
  @opsize_size 64

  @type t :: {fd :: tuple(), length :: non_neg_integer()}

  @spec open(Path.t(), ({pos_integer(), [term()]} -> any())) :: t()
  def open(full_wal_path, hook) do
    {:ok, wal_fd} = Fs.open(full_wal_path, [:read, :append, :write])

    total_length =
      wal_fd
      |> fd_stream()
      |> Enum.reduce(0, fn {batch_length, _batch} = entry, total_length ->
        hook.(entry)
        total_length + batch_length
      end)

    {wal_fd, total_length}
  end

  @spec push_batch(t(), [term()], non_neg_integer()) :: t()
  def push_batch(wal, ops, ops_length) do
    {fd, length} = wal

    encoded = for op <- ops, do: encode_op(op)
    :ok = Fs.write(fd, encoded)
    :ok = Fs.datasync(fd)

    {fd, length + ops_length}
  end

  @spec length(t()) :: non_neg_integer()
  def length(wal) do
    {_fd, length} = wal
    length
  end

  @spec truncate(t()) :: t()
  def truncate(wal) do
    {fd, _} = wal
    {:ok, 0} = Fs.position(fd, 0)
    :ok = Fs.truncate(fd)
    :ok = Fs.sync(fd)
    {fd, 0}
  end

  @spec stream(t()) :: Enumerable.t()
  def stream(wal) do
    {wal_fd, _} = wal
    fd_stream(wal_fd)
  end

  defp fd_stream(fd) do
    Stream.resource(
      fn -> {0, <<>>} end,
      fn {offset, head} ->
        case Fs.pread(fd, offset, @batch_read_size) do
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

  defp decode_ops(binary, acc, total_size, total_count) do
    case binary do
      <<size::@opsize_size, op::binary-size(size), tail::binary>> ->
        decode_ops(tail, [binary_to_term(op) | acc], total_size + size + 8, total_count + 1)

      binary ->
        {:lists.reverse(acc), binary, total_size + byte_size(binary), total_count}
    end
  end

  defp encode_op(op) do
    encoded = term_to_iovec(op)
    size = iolist_size(encoded)
    SizeException.check!(size, 1 <<< @opsize_size, op)
    [<<size::@opsize_size>> | encoded]
  end
end
