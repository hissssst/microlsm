defmodule Helpers do
  def gen_table(name, opts \\ []) do
    dir = Path.join [System.tmp_dir!(), to_string(name)]
    File.rm_rf(dir)
    File.mkdir(dir)
    default_opts = [name: name, data_dir: dir, threshold: 1000, block_count: 100, max_batch_length: 100]
    opts = Keyword.merge(default_opts, opts)
    {:ok, pid} = Microlsm.start_link(opts)
    pid
  end

  def fill_table(name, range \\ 1..1000) do
    batch =
      for i <- range do
        {:write, i, "value_#{i}"}
      end

    Microlsm.batch(name, batch)
  end
end
