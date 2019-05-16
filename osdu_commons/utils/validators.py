import attr


@attr.s(repr=False, slots=True, hash=True)
class _ListOfValidator:
    validator = attr.ib()

    def __call__(self, inst, attr, value):
        if not isinstance(value, list):
            raise TypeError(f'List expected, got {value.__class__}')

        for item in value:
            self.validator(inst, attr, item)

    def __repr__(self):
        return f'<list_of validator for {self.validator}'


def list_of(validator):
    return _ListOfValidator(validator)
