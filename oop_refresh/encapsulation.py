class Employee:
    def __init__(self, name, age):
        self.name = name
        self.age = age
        """
        _ protected attribute, should not be accessed
        __ private attribute and it cannot be accessed
        """
        self._salary = None
        self._gender = None 

    @property
    def salary(self):
        return self._salary

    @salary.setter
    def salary(self, value):
        self._salary = value

    @salary.deleter
    def salary(self):
        del self._salary

e1 = Employee("pedro", 23)
e1.salary = 30000
print(e1.salary)
del e1.salary
print(e1.salary)

