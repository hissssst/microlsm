defmodule Microlsm.Disktable do
  @moduledoc false

  alias Microlsm.BloomFilter
  alias Microlsm.Fs
  alias Microlsm.SizeException

  import :erlang, only: [binary_to_term: 1, element: 2, iolist_size: 1, term_to_iovec: 1]
  import Bitwise, only: [<<<: 2]

  require ODCounter

  @keyread_size 4 * 1024

  @header_read_size 4 * 1024

  @keysize_size 32

  @valuesize_size 32

  @pairprefix_size div(@keysize_size + @valuesize_size, 8)

  @type index :: :erlang.tuple()

  defguard is_inbound(x) when x == :in or element(1, x) == :exact

  ## Reading and streaming

  def total_length(fd) do
    {:ready, length, _max_block_size, _block_offsets_offset} = read_header(fd)
    length
  end

  def header_and_stream(fd) do
    {:ready, length, max_block_size, block_offsets_offset} = read_header(fd)

    stream =
      fn acc, fun ->
        {_block_count, block_offsets} = read_footer(fd, block_offsets_offset, @header_read_size)
        do_stream(block_offsets, fd, acc, fun)
      end

    {length, max_block_size, stream}
  end

  def stream(fd) do
    fn acc, fun ->
      {:ready, _length, _max_block_size, block_offsets_offset} = read_header(fd)
      {_block_count, block_offsets} = read_footer(fd, block_offsets_offset, @header_read_size)
      do_stream(block_offsets, fd, acc, fun)
    end
  end

  def stream(fd, index) do
    block_offsets =
      for {_key, offset} <- Tuple.to_list(index) do
        offset
      end

    fn acc, fun ->
      do_stream(block_offsets, fd, acc, fun)
    end
  end

  defp do_stream(block_offsets, fd, {:suspend, iacc}, fun) do
    cont = fn acc -> do_stream(block_offsets, fd, acc, fun) end
    {:suspended, iacc, cont}
  end

  defp do_stream(_block_offsets, _fd, {:halt, iacc}, _fun) do
    {:halted, iacc}
  end

  defp do_stream([last_offset], fd, {:cont, iacc}, fun) do
    acc = fun.(read_kv(fd, last_offset, @keyread_size), iacc)
    do_stream([], fd, acc, fun)
  end

  defp do_stream([], _fd, {:cont, iacc}, _fun) do
    {:done, iacc}
  end

  defp do_stream([first_offset, second_offset | block_offsets], fd, {:cont, iacc}, fun) do
    size = second_offset - first_offset
    {:ok, block} = Fs.pread(fd, first_offset, size)
    pairs = list_block(block)
    block_offsets = [second_offset | block_offsets]
    do_stream_items(pairs, block_offsets, fd, {:cont, iacc}, fun)
  end

  defp do_stream_items(pairs, block_offsets, fd, {:suspend, iacc}, fun) do
    cont = fn acc -> do_stream_items(pairs, block_offsets, fd, acc, fun) end
    {:suspended, iacc, cont}
  end

  defp do_stream_items(_pairs, _block_offsets, _fd, {:halt, iacc}, _fun) do
    {:halted, iacc}
  end

  defp do_stream_items([], block_offsets, fd, {:cont, iacc}, fun) do
    do_stream(block_offsets, fd, {:cont, iacc}, fun)
  end

  defp do_stream_items([pair | pairs], block_offsets, fd, {:cont, iacc}, fun) do
    acc = fun.(pair, iacc)
    do_stream_items(pairs, block_offsets, fd, acc, fun)
  end

  def range_offsets(index, left_key, right_key) do
    left_num =
      case find_num_block(index, left_key) do
        {:exact, num, _offset} -> num
        {:range, num, _, _, _} -> num
        {:error, :too_small} -> 1
        {:error, :too_big} -> throw :left_too_big
      end

    right_num =
      case find_num_block(index, right_key) do
        {:exact, num, _offset} -> num
        {:range, _, _, num, _} -> num
        {:error, :too_small} -> throw :right_too_small
        {:error, :too_big} -> tuple_size(index)
      end

    index_to_range(index, left_num, right_num)
  catch
    :left_too_big -> []
    :right_too_small -> []
  end

  defp index_to_range(index, i, i) do
    offset = element(2, element(i, index))
    [offset]
  end

  defp index_to_range(index, i, size) do
    offset = element(2, element(i, index))
    [offset | index_to_range(index, i + 1, size)]
  end

  def range_stream(_fd, _left_key, _right_key, [], _max_block_size) do
    []
  end

  def range_stream(fd, left_key, right_key, offsets, max_block_size) do
    stream =
      Stream.resource(
        fn -> {:first, offsets} end,
        fn
          [] ->
            {:halt, []}

          # Leftmost part
          {:first, [only_offset]} ->
            {[], [only_offset]}

          {:first, [first_offset, second_offset | block_offsets]} ->
            size = second_offset - first_offset
            {:ok, block} = Fs.pread(fd, first_offset, size)

            pairs =
              block
              |> list_block()
              |> filter_pairs_after(left_key)

            case block_offsets do
              [] ->
                # If this was the only part in offset list
                pairs = filter_pairs_before(pairs, right_key)
                {pairs, [second_offset]}

              _ ->
                {pairs, [second_offset | block_offsets]}
            end

          # Rightmost part
          [prelast_offset, last_offset] ->
            size = last_offset - prelast_offset
            {:ok, block} = Fs.pread(fd, prelast_offset, size)

            pairs =
              block
              |> list_block()
              |> filter_pairs_before(right_key)

            {pairs, [last_offset]}

          [only_offset] ->
            {key, _} = pair = read_kv(fd, only_offset, max_block_size)

            if key >= left_key and key <= right_key do
              {[pair], []}
            else
              {:halt, []}
            end

          # Inneer blocks
          [first_offset, second_offset | block_offsets] ->
            size = second_offset - first_offset
            {:ok, block} = Fs.pread(fd, first_offset, size)
            {list_block(block), [second_offset | block_offsets]}
        end,
        fn _ -> :ok end
      )

    stream
  end

  defp filter_pairs_after([{key, _value} | tail], min_key) when key < min_key do
    filter_pairs_after(tail, min_key)
  end

  defp filter_pairs_after(pairs, _min_key) do
    pairs
  end

  defp filter_pairs_before([{key, value} | tail], top_key) when key <= top_key do
    [{key, value} | filter_pairs_before(tail, top_key)]
  end

  defp filter_pairs_before(_, _top_key) do
    []
  end

  defp list_block(block) do
    case block do
      <<
        keysize::@keysize_size,
        valuesize::@valuesize_size,
        encoded_key::binary-size(keysize),
        encoded_value::binary-size(valuesize),
        rest::binary
      >> ->
        [{binary_to_term(encoded_key), encoded_value} | list_block(rest)]

      <<>> ->
        []
    end
  end

  defp read_kv(fd, offset, read_size) do
    {:ok, binary} = Fs.pread(fd, offset, read_size)
    case binary do
      <<keysize::@keysize_size, valuesize::@valuesize_size, encoded_key::binary-size(keysize), encoded_value::binary-size(valuesize), _::binary>> ->
        {binary_to_term(encoded_key), encoded_value}

      <<keysize::@keysize_size, valuesize::@valuesize_size, _::binary>> ->
        read_kv(fd, offset, @pairprefix_size + keysize + valuesize)
    end
  end

  ## Writing

  def truncate(fd) do
    :ok = Fs.truncate(fd)
    :ok = Fs.sync(fd)
  end

  def write_stream(stream, fd, approx_length, block_size_threshold) do
    offset = write_header(fd, 0, 0, block_size_threshold, :not_ready)
    bloom_filter_builder = BloomFilter.new(10 * approx_length, 6)

    {last_block, last_block_size, offset, first_key, last_key, key_to_blocks, len, bloom_filter_builder, max_block_size} =
      Enum.reduce(
        stream,
        {[], 0, offset, nil, nil, [], 0, bloom_filter_builder, 0},
        fn {key, encoded_value}, {block, block_size, offset, first_key, _last_key, key_to_blocks, len, bloom_filter_builder, max_block_size} ->
          len = len + 1

          bloom_filter_builder = BloomFilter.add(bloom_filter_builder, key)

          encoded_key = term_to_iovec(key)
          key_size = iolist_size(encoded_key)
          value_size = iolist_size(encoded_value)

          SizeException.check!(key_size, unquote(1 <<< @keysize_size), key)
          SizeException.check!(value_size, unquote(1 <<< @valuesize_size))

          entry = [<<key_size::@keysize_size, value_size::@valuesize_size>>, encoded_key, encoded_value]
          entry_size = key_size + value_size + @pairprefix_size
          block_and_entry_size = block_size + entry_size

          if block_and_entry_size > block_size_threshold do
            encoded_block = :lists.reverse(block)
            pwrite(fd, offset, encoded_block)
            key_to_blocks = [{first_key, offset} | key_to_blocks]
            max_block_size = max(block_size, max_block_size)

            {[entry], entry_size, offset + block_size, key, key, key_to_blocks, len, bloom_filter_builder, max_block_size}
          else
            first_key =
              case block do
                [] -> key
                _ -> first_key
              end

            {[entry | block], block_and_entry_size, offset, first_key, key, key_to_blocks, len, bloom_filter_builder, max_block_size}
          end
        end
      )

    max_block_size = max(last_block_size, max_block_size)

    [[_, _, last_encoded_value] | _] = last_block
    encoded_last_block = :lists.reverse(last_block)
    pwrite(fd, offset, encoded_last_block)
    key_to_blocks = [{first_key, offset} | key_to_blocks]
    offset = offset + last_block_size

    key_to_blocks =
      case last_block do
        [_] ->
          key_to_blocks

        _ ->
          last_encoded_key = term_to_iovec(last_key)
          last_pair_size = iolist_size(last_encoded_key) + iolist_size(last_encoded_value) + @pairprefix_size
          last_block = {last_key, offset - last_pair_size}

          [last_block | key_to_blocks]
      end
      |> :lists.reverse()

    index = index(key_to_blocks)
    block_offsets = Enum.map(key_to_blocks, fn {_, offset} -> offset end)
    write_footer(fd, offset, block_offsets)
    Fs.datasync(fd)

    write_header(fd, offset, len, max_block_size, :ready)
    Fs.sync(fd)

    bloom_filter = BloomFilter.finalize(bloom_filter_builder)
    {index, bloom_filter, max_block_size, len}
  end

  ## Indexing

  def load(fd) do
    case read_header(fd) do
      {:ready, length, max_block_size, block_offsets_offset} ->
        {_block_count, block_offsets} = read_footer(fd, block_offsets_offset, @header_read_size)
        {length, max_block_size, blocks_to_index(fd, block_offsets)}

      _ ->
        :broken
    end
  end

  defp blocks_to_index(fd, block_offsets) do
    key_to_blocks =
      for block_offset <- block_offsets do
        {key, _} = read_kv(fd, block_offset, @keyread_size)
        {key, block_offset}
      end

    index(key_to_blocks)
  end

  defp index(key_to_blocks) do
    List.to_tuple(key_to_blocks)
  end

  ## Search

  def find_block(index, key) do
    case find_num_block(index, key) do
      {:exact, _num, offset} ->
        {:exact, offset}

      {:range, _lnum, loffset, _rnum, roffset} ->
        {:range, loffset, roffset}

      other ->
        other
    end
  end

  def find_num_block(index, key) do
    size = tuple_size(index)

    case {element(1, index), element(size, index)} do
      {{left_key, _}, {right_key, _}} when key > left_key and key < right_key ->
        do_find_block(index, key, 1, size)

      {{^key, block_offset}, {_right_key, _}} ->
        {:exact, 1, block_offset}

      {{_left_key, _}, {^key, block_offset}} ->
        {:exact, size, block_offset}

      {{left_key, _}, _} when key < left_key ->
        {:error, :too_small}

      {_, {right_key, _}} when key > right_key ->
        {:error, :too_big}
    end
  end

  def check_bounds(index, key) do
    case {element(1, index), element(tuple_size(index), index)} do
      {{left_key, _}, {right_key, _}} when key > left_key and key < right_key ->
        :in

      {{^key, block_offset}, {_right_key, _}} ->
        {:exact, block_offset}

      {{_left_key, _}, {^key, block_offset}} ->
        {:exact, block_offset}

      {{left_key, _}, _} when key < left_key ->
        :lower

      {_, {right_key, _}} when key > right_key ->
        :higher
    end
  end

  defp do_find_block(index, key, left, right) do
    middle = div(left + right, 2)
    {middle_key, middle_block} = element(middle, index)

    case middle do
      ^left ->
        case middle_key do
          ^key ->
            {:exact, middle, middle_block}

          _ ->
            {_, right_block} = element(right, index)
            {:range, middle, middle_block, middle + 1, right_block}
        end

      _ ->
        case middle_key do
          ^key ->
            {:exact, middle, middle_block}

          middle_key when middle_key < key ->
            do_find_block(index, key, middle, right)

          middle_key when middle_key > key ->
            do_find_block(index, key, left, middle)
        end
    end
  end

  def find_value(fd, find_block_result, key, read_size) do
    case find_block_result do
      {:exact, offset} ->
        {_key, encoded_value} = read_kv(fd, offset, read_size)
        {:ok, binary_to_term(encoded_value)}

      {:range, left, right} ->
        {:ok, chunk} = Fs.pread(fd, left, right - left)
        find_in_block(chunk, key)

      other ->
        other
    end
  end

  def find_in_block(block, key) do
    find_in_chunk(block, key)
  end

  def find_in_chunk(chunk, key) do
    case chunk do
      <<
        keysize::@keysize_size,
        valuesize::@valuesize_size,
        encoded_key::binary-size(keysize),
        encoded_value::binary-size(valuesize),
        rest::binary
      >> ->
        case binary_to_term(encoded_key) do
          ^key ->
            {:ok, binary_to_term(encoded_value)}

          higher when higher > key ->
            {:error, :not_found_in_chunk_early}

          _ ->
            find_in_chunk(rest, key)
        end

      <<>> ->
        {:error, :not_found_in_chunk}
    end
  end

  ## Header

  @offset_size 64

  @magic "disktabl"
  64 = bit_size(@magic)

  defp write_header(fd, block_offsets_offset, len, max_block_size, ready) do
    magic_readiness =
      case ready do
        :not_ready -> 0b0000_0011_0011_0011_0011_0011_0011_0000
        :ready ->     0b1111_1100_1100_1100_1100_1100_1100_1111
      end

    header = <<@magic, magic_readiness :: 32, len::64, max_block_size::64, block_offsets_offset::64>>
    :ok = pwrite(fd, 0, header)
    byte_size(header)
  end

  defp read_header(fd) do
    case Fs.pread(fd, 0, 36) do
      {:ok, <<@magic, magic_readiness :: 32, len::64, max_block_size::64, block_offsets_offset::64>>} ->
        ready =
          case magic_readiness do
            0b0000_0011_0011_0011_0011_0011_0011_0000 -> :not_ready
            0b1111_1100_1100_1100_1100_1100_1100_1111 -> :ready
          end

        {ready, len, max_block_size, block_offsets_offset}

      _other ->
        #TODO implement logging
        :broken
    end
  end

  defp write_footer(fd, block_offsets_offset, block_offsets) do
    block_count = length(block_offsets)
    footer = [<<block_count::64>> | encode_offsets(block_offsets)]
    :ok = pwrite(fd, block_offsets_offset, footer)
  end

  defp encode_offsets([offset]) do
    [<<offset::@offset_size>>]
  end

  defp encode_offsets([offset | rest]) do
    [<<offset::@offset_size>> | encode_offsets(rest)]
  end

  defp read_footer(fd, block_offsets_offset, readsize) do
    {:ok, <<block_count::64, rest::binary>>} = Fs.pread(fd, block_offsets_offset, readsize)
    block_offsets =
      if byte_size(rest) < 8 * block_count do
        {:ok, block_offsets} = Fs.pread(fd, block_offsets_offset + 8, 8 * block_count)
        block_offsets
      else
        rest
      end

    {block_count, parse_offsets_array(block_offsets, block_count)}
  end

  defp parse_offsets_array(_binary, 0) do
    []
  end

  defp parse_offsets_array(<<i::@offset_size, rest::binary>>, len) do
    [i | parse_offsets_array(rest, len - 1)]
  end

  defp pwrite(fd, offset, iovec) do
    name = Process.get(:microlsm_name)
    ODCounter.add(Microlsm, name, :pwrites)
    Fs.pwrite(fd, offset, iovec)
  end
end
