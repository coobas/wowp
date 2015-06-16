from nose.tools import istest, nottest

try:
    from wowp.actors.matlab import MatlabMethod
    matlab_test = istest
except Exception:
    matlab_test = nottest


@matlab_test
def test_MatlabMethod_call():
    mm = MatlabMethod('ceil')
    assert mm(3.1) == 4.0
