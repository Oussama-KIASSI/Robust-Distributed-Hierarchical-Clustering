class Point:

    def __init__(self, p_id=0, coords=None):
        self.id = p_id
        self.coords = coords.copy()

    @classmethod
    def from_point(cls, other):
        return cls(other.id, other.coords.copy())

    def set(self, id, coords):
        self.id = id
        self.coords = coords

    def setCoords(self, coords):
        self.coords = coords

    def setId(self, id):
        self.id = id

    def getDimension(self):
        return len(self.coords)

    def getCoords(self):
        return self.coords

    def getId(self):
        return self.id

    def distanceTo(self, other):
        if other.getId() == self.id:
            return 0
        d = 0
        for i in range(len(self.coords)):
            d += (self.coords[i] - other.getCoords()[i]) ** 2
        return d ** 0.5

    def __eq__(self, other):
        if not isinstance(other, Point):
            raise RuntimeError("other has wrong type")
        otherP = other
        if self.id != otherP.getId():
            return False
        return self.coords == otherP.getCoords()

    def __hash__(self):
        return self.id * 163 + len(self.coords) * 57 + int(sum(self.coords))

    def __str__(self):
        return str(self.id) + ": " + str(self.coords)

    def __repr__(self):
        return self.__str__()

    def read(self, stream):
        self.id = stream.readInt()
        self.coords = stream.readObject()

    def write(self, stream):
        stream.writeInt(self.id)
        stream.writeObject(self.coords)

    def __clone__(self):
        return Point(self.id, self.coords.copy())

    def serialize(self):
        return str(self.id) + "," + ",".join([str(c) for c in self.coords])

    @staticmethod
    def deserialize(s):
        parts = s.split(",")
        return Point(int(parts[0]), [int(parts[i]) for i in range(1, len(parts))])

"""def read(self, kryo, input):
        self.id = input.readInt(True)
        self.coords = kryo.readClassAndObject(input)

    def write(self, kryo, output):
        output.writeInt(self.id, True)
        kryo.writeClassAndObject(output, self.coords)"""
