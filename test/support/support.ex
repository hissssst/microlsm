defmodule Microlsm.Test.Support do
  def setup_datadir do
    unique = :erlang.unique_integer([:positive])
    name = :"microlsm_#{unique}"
    data_dir = Path.join(System.tmp_dir!(), "microlsm_test_#{unique}")
    File.rm_rf(data_dir)
    File.mkdir(data_dir)

    %{name: name, data_dir: data_dir}
  end
end
