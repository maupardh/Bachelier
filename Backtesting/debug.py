


class A:
    def __init__(self):
        self.prop1 = 3
        self.prop2 = [4,6,2]

class B(A):
    def __init__(self):
        A.__init__(self)
        self.prop3 = {'ta':8}

    def print_(self):
        for (att, value) in self.__dict__.items():
            print (att, value)


instance_b = B()
print isinstance(instance_b, B)
print isinstance(instance_b, A)