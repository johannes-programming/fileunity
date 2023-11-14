import tomllib as _tomllib

import tomli_w as _tomli_w


class BaseUnit:
    def __init__(self, data):
        self.data = data
    @property
    def data(self):
        return self.data_duplicating(self._data)
    @data.setter
    def data(self):
        return self.data_duplicating(self._data)
    @classmethod
    def load(cls, file):
        return cls(cls.data_loading(file))
    def save(self, file):
        self.data_saving(file, self._data)

class PrintableUnit(BaseUnit):
    pass

class TextUnit(PrintableUnit):
    @classmethod
    def by_str(cls, string):
        return cls([string])
    @classmethod
    def data_duplicating(data):
        ans = list()
        for x in data:
            ans += str(x).split('\n')
        return ans
    @classmethod
    def data_loading(cls, file):
        with open(file, "r") as stream:
            contents = stream.read()
        if contents.endswith('\n'):
            contents = contents[:-1]
        return [contents]
    @classmethod
    def data_saving(cls, file, data):
        with open(file, "w") as stream:
            stream.write('\n'.join(data) + '\n')
    def __getitem__(self, key):
        return self.data[key]
    def __setitem__(self, key, value):
        cls = type(self)
        data = self.data
        data[key] = value
        return cls(data)
    def __delitem__(self, key):
        cls = type(self)
        data = self.data
        del data[key]
        return cls(data)
    def __iter__(self):
        return (x for x in self._data)
    def __len__(self):
        return len(self._data)
    def __str__(self):
        return '\n'.join(self._data)
    def __add__(self, other):
        cls = type(self)
        other = cls(other)
        return cls(self._data + other._data)
    def __radd__(self, other):
        cls = type(self)
        other = cls(other)
        return cls(other._data + self._data)
    def __mul__(self, other):
        cls = type(self)
        data = self._data * other
        return cls(data)
    def __rmul__(self, other):
        cls = type(self)
        data = other * self._data
        return cls(data)
    def __contains__(self, other):
        return (other in self._data)


def TOMLUnit(PrintableUnit):
    @classmethod
    def data_duplicating(cls, data):
        string = _tomli_w.dumps(data)
        ans = _tomllib.loads(string)
        return ans
    @classmethod
    def data_loading(cls, file):
        with open(file, "rb") as stream:
            return _tomllib.load(stream)
    @classmethod
    def data_saving(cls, file, data):
        with open(file, "wb") as stream:
            return _tomli_w.dump(stream)
    def __getitem__(self, key):
        return self.data[key]
    def __setitem__(self, key, value):
        cls = type(self)
        data = self.data
        data[key] = value
        return cls(data)
    def __delitem__(self, key):
        cls = type(self)
        data = self.data
        del data[key]
        return cls(data)
    def __len__(self):
        return len(self._data)
    def keys(self):
        x = self._data.keys()
        x = list(x)
        x = (y for y in x)
        return x
    def values(self):
        x = self._data.values()
        x = list(x)
        x = (y for y in x)
        return x
    def items(self):
        x = self._data.items()
        x = list(x)
        x = (y for y in x)
        return x
    def __str__(self):
        return _tomli_w.dumps(data)
    def __add__(self, other):
        cls = type(self)
        other = cls(other)
        x = dict(**self._data, **other._data)
        return cls(x)
    def __radd__(self, other):
        cls = type(self)
        other = cls(other)
        x = dict(**other._data, **self._data)
        return cls(x)




 
