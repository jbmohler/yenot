import rtlib


class XMixin:
    def testing(self):
        return "hi"


def test_mixin():
    MyClass = rtlib.fixedrecord("MyClass", ["name", "age"], mixin=XMixin)
    x = MyClass("joel", 25)
    assert x.testing() == "hi"
    assert x.age == 25
