import os
import sys
import pytest

# Modify the path so that modules in the project root can be imported without installing
test_dir = os.path.dirname(__file__)
root_dir = os.path.dirname(test_dir)
sys.path.append(root_dir)

from lib.ortho_functions import get_destination_nodata, OutputType

def test_get_destination_nodata_succeeds():
    assert get_destination_nodata(output_type=OutputType.BYTE) == 0
    assert get_destination_nodata(output_type="Byte") == 0

    assert get_destination_nodata(output_type=OutputType.UINT16) == 65535
    assert get_destination_nodata(output_type="UInt16") == 65535

    assert get_destination_nodata(output_type=OutputType.FLOAT32) == -9999.0
    assert get_destination_nodata(output_type="Float32") == -9999.0

def test_get_destination_nodata_raises():
    with pytest.raises(ValueError):
        get_destination_nodata(output_type="not a valid output type")