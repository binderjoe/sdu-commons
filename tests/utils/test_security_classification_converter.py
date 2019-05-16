import pytest

from osdu_commons.utils.convert import resource_security_classification
from osdu_commons.utils.srn import SRN

RESTRICTED_SRN = SRN(type='reference-data/ResourceSecurityClassification', detail='RESTRICTED')
CLASSIFIED_SRN = SRN(type='reference-data/ResourceSecurityClassification', detail='CLASSIFIED')
CONFIDENTIAL_SRN = SRN(type='reference-data/ResourceSecurityClassification', detail='CONFIDENTIAL')
MOST_CONFIDENTIAL = SRN(type='reference-data/ResourceSecurityClassification', detail='MOST-CONFIDENTIAL')


@pytest.mark.parametrize("valid_input,expected_output", [
    (RESTRICTED_SRN, RESTRICTED_SRN),
    (CLASSIFIED_SRN, CLASSIFIED_SRN),
    (CONFIDENTIAL_SRN, CONFIDENTIAL_SRN),
    (MOST_CONFIDENTIAL, MOST_CONFIDENTIAL),
    ('RESTRICTED', RESTRICTED_SRN),
    ('CLASSIFIED', CLASSIFIED_SRN),
    ('CONFIDENTIAL', CONFIDENTIAL_SRN),
    ('MOST-CONFIDENTIAL', MOST_CONFIDENTIAL),
    ('srn:reference-data/ResourceSecurityClassification:RESTRICTED:', RESTRICTED_SRN),
    ('srn:reference-data/ResourceSecurityClassification:CLASSIFIED:', CLASSIFIED_SRN),
    ('srn:reference-data/ResourceSecurityClassification:CONFIDENTIAL:', CONFIDENTIAL_SRN),
    ('srn:reference-data/ResourceSecurityClassification:MOST-CONFIDENTIAL:', MOST_CONFIDENTIAL)]
)
def test_resource_security_classification_correct(valid_input, expected_output):
    output = resource_security_classification(valid_input)
    assert output == expected_output


@pytest.mark.parametrize("bad_input", [
    SRN(type='wrong_type', detail='eyeye'),
    SRN(type='reference-data/ResourceSecurityClassification', detail='RESTRICTED', version=1),
    SRN(type='reference-data/ResourceSecurityClassification', detail='CLASSIFIED', version=1),
    SRN(type='reference-data/ResourceSecurityClassification', detail='CONFIDENTIAL', version=1),
    SRN(type='reference-data/ResourceSecurityClassification', detail='MOST-CONFIDENTIAL', version=1),
    'UNKNOWN_TYPE',
    'srn:reference-data/ResourceSecurityClassification:RESTRICTED:1',
    'srn:reference-data/ResourceSecurityClassification:CLASSIFIED:1',
    'srn:reference-data/ResourceSecurityClassification:CONFIDENTIAL:1',
    'srn:reference-data/ResourceSecurityClassification:MOST-CONFIDENTIAL:1',
    'srn:reference-data/ResourceSecurityClassification:UNKNOWN_TYPE:1',
    'srn:wrong_type:MOST-CONFIDENTIAL:1',
]
)
def test_resource_security_classification_bad(bad_input):
    with pytest.raises(TypeError):
        resource_security_classification(bad_input)
