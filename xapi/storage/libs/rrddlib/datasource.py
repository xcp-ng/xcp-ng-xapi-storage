INT64_MAX = 9223372036854775807
INT64_MIN = -INT64_MAX - 1
# DBL_MAX = 1.79769e+308
# DBL_MIN = -DBL_MAX
INF = float('inf')
N_INF = float('-inf')

# Accept old and new ds_types until everyone moves to the new ones
DS_TYPE_VALS = frozenset(('gauge', 'absolute', 'derive',
                          'rate', 'absolute_to_rate'))
VAL_TYPE_VALS = frozenset(('float', 'int64'))

# Mapping of datasource value types to C types
DS_TO_C_TYPE = {
    'int64': 'int64_t',
    'float': 'double'
}

# 'name' and 'value' do not really have default values,
# since it's impossible to create a Datasource object
# without them. They are put here for convenience.
DEFAULTS = {
    'name': '<empty>',
    'value': 0.0,
    'description': '',
    'type': 'gauge',
    'min': N_INF,
    'max': INF,
    'units': '',
    'value_type': 'float',
    'owner': 'host'
}

# TODO: move this to some kind of
#       generic helpers file


def is_valid_c_primitive(obj, c_type):
    """Get number if 'obj' is a valid C <c_type>; else 'None'.

    Checks if 'obj' can be cast into its C <c_type> and if its
    value is within the C range for that type.

    Args:
        obj: a python object
        c_type: string with the name of the C type we wish to
            check against.

    Returns:
        num: python 'int' or 'float' object if 'obj' is a valid
            C <c_type> primitive
        None: if 'obj' is not valid C <c_type> primitive

    Raises:
        ValueError: 'c_type' is not a valid C primitive type name
    """

    num = 0

    if c_type == 'int64_t':
        cast = int
        num_min = INT64_MIN
        num_max = INT64_MAX
    elif c_type == 'double':
        cast = float
        num_min = N_INF  # pylint: disable=redefined-variable-type
        num_max = INF  # pylint: disable=redefined-variable-type
    else:
        raise ValueError(c_type + ": Unsupported <c_type> value")

    try:
        num = cast(obj)
        if not (isinstance(obj, str) or isinstance(obj, cast)):
            return None
    except ValueError:
        return None
    else:
        if num < num_min or num > num_max:
            return None

    return num

# TODO: move this to some kind of
#       generic helpers file


def is_valid_uuid4(uuid_str):
    from uuid import UUID

    try:
        val = UUID(uuid_str, version=4)
    except ValueError:
        return False

    # Check that the hex representation of the generated
    # UUID4 is exactly the same as the input string,
    # with dashes and curly braces removed and lowercased.
    # N.B.: it is valid for a uuid string to start with
    # 'urn:uuid:'. However, this method does not account
    # for this and will return 'False' if this is the case
    return val.hex == uuid_str.translate(None, '{}-').lower()


class Datasource(object):
    def __init__(
            self,
            name,
            value,
            value_type,
            description=None,
            datasource_type=None,
            min_val=None,
            max_val=None,
            units=None,
            owner=None):

        self.__data = {}

        # In Python 3.x, check against 'str'
        if not isinstance(name, str):
            raise TypeError("'name' is not of type 'string'")
        self.__data['name'] = name

        if value_type not in VAL_TYPE_VALS:
            raise ValueError(
                "'value_type' not one of '{}'".format(
                    ', '.join(VAL_TYPE_VALS)
                )
            )
        self.__data['value_type'] = value_type

        c_val_type = 'int64_t' if value_type == 'int64' else 'double'

        tmp_1 = is_valid_c_primitive(value, c_val_type)
        if tmp_1 is None:
            raise ValueError("'{}' not of type '{}'".format(value, c_val_type))
        self.__data['value'] = tmp_1

        if description is not None:
            if not isinstance(description, str):
                raise TypeError("'description' is not of type 'string'")
            self.__data['description'] = description

        if datasource_type is not None:
            if datasource_type not in DS_TYPE_VALS:
                raise ValueError(
                    "'datasource_type' not one of '{}'".format(
                        ', '.join(DS_TYPE_VALS)
                    )
                )
            self.__data['type'] = datasource_type

        if units is not None:
            if not isinstance(units, str):
                raise TypeError("'units' is not of type 'string'")
            self.__data['units'] = units

        if min_val is not None:
            tmp_1 = is_valid_c_primitive(min_val, c_val_type)
            if tmp_1 is None:
                raise TypeError(
                    "'{}' not of type '{}'".format(min_val, c_val_type)
                )
            self.__data['min'] = tmp_1
        else:
            tmp_1 = INT64_MIN if c_val_type == 'int64_t' else N_INF

        if max_val is not None:
            tmp_2 = is_valid_c_primitive(max_val, c_val_type)
            if tmp_2 is None:
                raise TypeError(
                    "'{}' not of type '{}'".format(max_val, c_val_type)
                )
            self.__data['max'] = tmp_2
        else:
            tmp_2 = INT64_MAX if c_val_type == 'int64_t' else INF

        if tmp_1 > tmp_2:
            raise ValueError(
                "'min_val': {} > 'max_val': {}".format(tmp_1, tmp_2)
            )

        if self.__data['value'] < tmp_1 or self.__data['value'] > tmp_2:
            # Closed intervals + infinities is wrong..
            # but honestly, couldn't be bothered
            raise ValueError(
                "'value' not in range: [{}, {}]".format(tmp_1, tmp_2)
            )

        if owner is not None:
            if not Datasource.__is_valid_owner(owner):
                raise ValueError("'owner' value is invalid: {}".format(owner))
            self.__data['owner'] = owner

    @staticmethod
    def __is_valid_owner(owner):
        tmp = owner.split(' ')
        if len(tmp) == 1 and tmp[0] == 'host':
            return True
        elif (len(tmp) == 2 and
                (tmp[0] == 'vm' or tmp[0] == 'sr') and
                is_valid_uuid4(tmp[1])):
            return True

        return False

    def to_dict(self):
        """Get Datasource's metadata {}.

        The returned {} should be parsed with JSONEncoder
        and appended to the plugin's metadata string.
        """
        return {
            self.__data['name']: {
                key: str(self.__data[key])
                for key in self.__data
                if key != 'name' and key != 'value'
            }
        }

    def get_property(self, prop_name):
        """Get value of Datasource property"""

        if prop_name not in DEFAULTS:
            raise KeyError(
                "'{}' not a Datasource property".format(prop_name)
            )

        if prop_name in self.__data:
            return self.__data[prop_name]
        else:
            return DEFAULTS[prop_name]

    def set_value(self, value):
        """Sets Datasource value."""
        val_type = self.get_property('value_type')

        tmp = is_valid_c_primitive(
            value,
            DS_TO_C_TYPE[val_type]
        )

        min_ = self.get_property('min')
        max_ = self.get_property('max')

        if tmp is None:
            raise ValueError("'{}' not of type '{}'".format(value, val_type))
        elif tmp < min_ or tmp > max_:
            raise ValueError(
                "'value' = {}; not in range: [{}, {}]".format(tmp, min_, max_)
            )

        self.__data['value'] = tmp
