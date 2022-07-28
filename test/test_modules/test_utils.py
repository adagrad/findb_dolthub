from modules.disk_utils import check_disk_full


def test_disk_space():
    assert not check_disk_full(), "Ups disk is full"
    assert check_disk_full(minimum="99TB"), "Ups disk is prety big"
