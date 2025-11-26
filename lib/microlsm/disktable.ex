defmodule Microlsm.Disktable do
  @moduledoc false
  import :erlang, only: [binary_to_term: 1, element: 2, iolist_size: 1, term_to_iovec: 1]

  @keyread_size 4 * 1024

  @header_read_size 4 * 1024

  @keysize_size 64

  @valuesize_size 64

  alias Microlsm.BloomFilter

  ## Reading and streaming

  def total_length(fd) do
    {length, _block_offsets} = read_header(fd, @header_read_size)
    length
  end

  def stream(fd) do
    fn acc, fun ->
      {_length, block_offsets} = read_header(fd, @header_read_size)
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
    {:ok, block} = :prim_file.pread(fd, first_offset, size)
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

  def range_stream(fd, index, left_key, right_key) do
    left_offset =
      case find_block(index, left_key) do
        {:exact, offset} -> offset
        {:range, offset, _} -> offset
        {:error, :too_small} -> 0
        {:error, :too_big} -> throw :left_too_big
      end

    right_offset =
      case find_block(index, right_key) do
        {:exact, offset} -> offset
        {:range, _, offset} -> offset
        {:error, :too_small} -> throw :right_too_small
        {:error, :too_big} ->
          {_key, last_offset} = element(tuple_size(index), index)
          last_offset
      end

    offsets =
      for {_, block_offset} when block_offset >= left_offset and block_offset <= right_offset <- Tuple.to_list(index) do
        block_offset
      end

    stream =
      Stream.resource(
        fn -> {:first, offsets} end,
        fn
          [] ->
            {:halt, []}

          # Leftmost part
          {:first, [only_offset]} ->
            kv = read_kv(fd, only_offset, 4 * 1024)
            {[kv], []}

          {:first, [first_offset, second_offset | block_offsets]} ->
            size = second_offset - first_offset
            {:ok, block} = :prim_file.pread(fd, first_offset, size)

            pairs =
              block
              |> list_block()
              |> filter_pairs_after(left_key)

            case block_offsets do
              [] ->
                # If this was the only part in offset list
                pairs = filter_pairs_before(pairs, right_key)
                {pairs, []}

              _ ->
                {pairs, [second_offset | block_offsets]}
            end

          # Rightmost part
          [prelast_offset, last_offset] ->
            size = last_offset - prelast_offset
            {:ok, block} = :prim_file.pread(fd, prelast_offset, size)

            pairs =
              block
              |> list_block()
              |> filter_pairs_before(right_key)

            {pairs, []}

          # Inneer blocks
          [first_offset, second_offset | block_offsets] ->
            size = second_offset - first_offset
            {:ok, block} = :prim_file.pread(fd, first_offset, size)
            {list_block(block), [second_offset | block_offsets]}
        end,
        fn _ -> :ok end
      )

    stream
  catch
    :left_too_big -> []
    :right_too_small -> []
  end

  defp filter_pairs_after([{key, _value} | tail], min_key) when key < min_key do
    filter_pairs_after(tail, min_key)
  end

  defp filter_pairs_after([{key, _value} | _] = pairs, min_key) when key >= min_key do
    pairs
  end

  defp filter_pairs_after([], _) do
    []
  end

  defp filter_pairs_before([{key, value} | tail], top_key) when key <= top_key do
    [{key, value} | filter_pairs_before(tail, top_key)]
  end

  defp filter_pairs_before([{key, _value} | _], top_key) when key > top_key do
    []
  end

  defp filter_pairs_before([], _) do
    []
  end

  defp list_block(block) do
    case block do
      <<keysize::@keysize_size, encoded_key::binary-size(keysize), valuesize::@valuesize_size,
        encoded_value::binary-size(valuesize), rest::binary>> ->
        [{binary_to_term(encoded_key), encoded_value} | list_block(rest)]

      <<>> ->
        []
    end
  end

  defp read_kv(fd, offset, read_size) do
    case :prim_file.pread(fd, offset, read_size) do
      {:ok,
       <<keysize::@keysize_size, encoded_key::binary-size(keysize), valuesize::@valuesize_size,
         encoded_value::binary-size(valuesize), _::binary>>} ->
        {binary_to_term(encoded_key), encoded_value}

      {:ok, <<keysize::@keysize_size, encoded_key::binary-size(keysize), valuesize::@valuesize_size, _::binary>>} ->
        {:ok, <<encoded_value::binary-size(valuesize)>>} =
          :prim_file.pread(fd, offset + 8 + keysize + 8, valuesize)

        {binary_to_term(encoded_key), encoded_value}

      {:ok, <<keysize::@keysize_size, _::binary>>} ->
        read_size = keysize + 16 + read_size

        case :prim_file.pread(fd, offset, read_size) do
          {:ok,
           <<keysize::@keysize_size, encoded_key::binary-size(keysize), valuesize::@valuesize_size,
             encoded_value::binary-size(valuesize), _::binary>>} ->
            {binary_to_term(encoded_key), encoded_value}

          {:ok, <<keysize::@keysize_size, encoded_key::binary-size(keysize), valuesize::@valuesize_size, _::binary>>} ->
            {:ok, <<encoded_value::binary-size(valuesize)>>} =
              :prim_file.pread(fd, offset + 8 + keysize + 8, valuesize)

            {binary_to_term(encoded_key), encoded_value}
        end
    end
  end

  ## Writing

  def write_stream(stream, fd, length, block_count) do
    nth = max(div(length, block_count), 10)
    offset = write_header(fd, List.duplicate(0, block_count * 2), 0)

    {offset, last_key, last_encoded_value, key_to_blocks, len, bloom_filter} =
      stream
      |> Stream.chunk_every(nth)
      |> Enum.reduce(
        {offset, nil, <<>>, [], 0, BloomFilter.new(10 * length, 6)},
        fn block_batch, {offset, _last_key, _last_encoded_value, key_to_blocks, len, bloom_filter} ->
          len = len + length(block_batch)
          {first_key, _} = hd(block_batch)
          key_to_blocks = [{first_key, offset} | key_to_blocks]

          {encoded_block, block_size, bloom_filter, last_key, last_encoded_value} = encode_block(block_batch, bloom_filter, [], 0)
          :prim_file.pwrite(fd, offset, encoded_block)

          {offset + block_size, last_key, last_encoded_value, key_to_blocks, len, bloom_filter}
        end
      )

    last_encoded_key = term_to_iovec(last_key)
    last_pair_size = iolist_size(last_encoded_key) + iolist_size(last_encoded_value) + 16
    last_block = {last_key, offset - last_pair_size}

    key_to_blocks =
      case key_to_blocks do
        [^last_block | _] = key_to_blocks -> key_to_blocks
        _ -> [last_block | key_to_blocks]
      end
      |> :lists.reverse()

    index = index(key_to_blocks)
    block_offsets = Enum.map(key_to_blocks, fn {_, offset} -> offset end)
    write_header(fd, block_offsets, len)
    :prim_file.datasync(fd)
    bloom_filter = BloomFilter.finalize(bloom_filter)
    {index, bloom_filter}
  end

  defp encode_block([{key, encoded_value}], bloom_filter, acc, block_size) do
    bloom_filter = BloomFilter.add(bloom_filter, key)
    encoded_key = term_to_iovec(key)
    key_size = iolist_size(encoded_key)
    value_size = iolist_size(encoded_value)
    block_size = block_size + key_size + value_size + 16
    entry = [<<key_size::@keysize_size>>, encoded_key, <<value_size::@valuesize_size>>, encoded_value]
    {:lists.reverse([entry | acc]), block_size, bloom_filter, key, encoded_value}
  end

  defp encode_block([{key, encoded_value} | pairs], bloom_filter, acc, block_size) do
    bloom_filter = BloomFilter.add(bloom_filter, key)
    encoded_key = term_to_iovec(key)
    key_size = iolist_size(encoded_key)
    value_size = iolist_size(encoded_value)
    block_size = block_size + key_size + value_size + 16
    entry = [<<key_size::@keysize_size>>, encoded_key, <<value_size::@valuesize_size>>, encoded_value]
    encode_block(pairs, bloom_filter, [entry | acc], block_size)
  end

  ## Indexing

  def load(fd) do
    {length, block_offsets} = read_header(fd, @header_read_size)
    {length, blocks_to_index(fd, block_offsets)}
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
    size = tuple_size(index)

    case {element(1, index), element(size, index)} do
      {{left_key, _}, {right_key, _}} when key > left_key and key < right_key ->
        do_find_block(index, key, 1, size)

      {{^key, block_offset}, {_right_key, _}} ->
        {:exact, block_offset}

      {{_left_key, _}, {^key, block_offset}} ->
        {:exact, block_offset}

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
            {:exact, middle_block}

          _ ->
            {_, right_block} = element(right, index)
            {:range, middle_block, right_block}
        end

      _ ->
        case middle_key do
          ^key ->
            {:exact, middle_block}

          middle_key when middle_key < key ->
            do_find_block(index, key, middle, right)

          middle_key when middle_key > key ->
            do_find_block(index, key, left, middle)
        end
    end
  end

  def find_value(fd, index, key, read_size \\ @keyread_size) do
    case find_block(index, key) do
      {:exact, offset} ->
        {_key, encoded_value} = read_kv(fd, offset, read_size)
        {:ok, binary_to_term(encoded_value)}

      {:range, left, right} ->
        {:ok, chunk} = :prim_file.pread(fd, left, right - left)
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
        encoded_key::binary-size(keysize),
        valuesize::@valuesize_size,
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

  defp write_header(fd, block_offsets, len) do
    block_count = length(block_offsets)
    header = [<<len::64, block_count::64>> | encode_offsets(block_offsets)]
    :ok = :prim_file.pwrite(fd, 0, header)
    block_count * 8 + 16
  end

  defp encode_offsets([offset]) do
    [<<offset::64>>]
  end

  defp encode_offsets([offset | rest]) do
    [<<offset::64>> | encode_offsets(rest)]
  end

  defp read_header(fd, readsize) do
    {:ok, <<len::64, block_count::64, rest::binary>>} = :prim_file.pread(fd, 0, readsize)

    block_offsets =
      if byte_size(rest) < 8 * block_count do
        {:ok, block_offsets} = :prim_file.pread(fd, 16, 8 * block_count)
        block_offsets
      else
        rest
      end

    {len, parse_array(block_offsets, block_count)}
  end

  defp parse_array(_binary, 0) do
    []
  end

  defp parse_array(<<i::64, rest::binary>>, len) do
    [i | parse_array(rest, len - 1)]
  end
end
