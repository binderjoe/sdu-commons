import pytest

from osdu_commons.utils.srn import SRN, SRNFormatException


@pytest.mark.parametrize('srn_string,srn_type,detail,version', [
    ('srn:type:/master-data/well/6b0543f0-25ae-4294-b6dc-ae5bc3db2640:1',
     'type',
     '/master-data/well/6b0543f0-25ae-4294-b6dc-ae5bc3db2640',
     1),
    ('srn:type:/abc/def:', 'type', '/abc/def', None),
    ('srn:type:/abc/def:', 'type', '/abc/def', None),
    ('srn:type:/abc/def:1', 'type', '/abc/def', 1),
])
def test_valid_srn(srn_string, srn_type, detail, version):
    srn = SRN.from_string(srn_string)

    assert srn.type == srn_type
    assert srn.detail == detail
    assert srn.version == version
    assert str(srn) == srn_string


def test_no_version_colon():
    with pytest.raises(SRNFormatException):
        SRN.from_string('srn:type:/abc/def')


def test_no_type():
    with pytest.raises(SRNFormatException):
        SRN.from_string('srn::/abc/def:')


def test_no_detail():
    with pytest.raises(SRNFormatException):
        SRN.from_string('srn:type::')


def test_too_many_sections():
    with pytest.raises(SRNFormatException):
        SRN.from_string('srn:type:/abc/def:1:xyz')


def test_version_not_a_number():
    with pytest.raises(SRNFormatException):
        SRN.from_string('srn:type:/abc/def:xyz')


def test_eq_when_equal():
    srn1 = SRN(type='type', detail='detail', version=1)
    srn2 = SRN(type='type', detail='detail', version=1)

    assert srn1 == srn2


def test_eq_when_not_equal():
    srn1 = SRN(type='type', detail='detail', version=1)
    srn2 = SRN(type='type', detail='detail')

    assert not srn1 == srn2


@pytest.mark.parametrize('value,expected', [
    (SRN(type='type', detail='detail', version=1), SRN(type='type', detail='detail')),
    (SRN(type='type', detail='detail'), SRN(type='type', detail='detail')),
])
def test_without_version(value, expected):
    assert value.without_version == expected


@pytest.mark.parametrize('value,new_version,expected', [
    (SRN(type='type', detail='detail', version=1), 5, SRN(type='type', detail='detail', version=5)),
    (SRN(type='type', detail='detail'), 5, SRN(type='type', detail='detail', version=5)),
])
def test_with_version(value, new_version, expected):
    assert value.with_version(new_version) == expected
