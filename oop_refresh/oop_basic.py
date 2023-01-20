class SoftwareEngineer():
    # class attributes
    alias = "keyboard magician"
         
    # instance or object attributes
    def __init__(self, name, age, level, salary):
        self.name = name
        self.age = age
        self.level = level
        self.salary = salary

    # instance or object method
    def code(self):
        print(f"{self.name} está escribiendo")

    def code_in_language(self, language):
        print(f"{self.name} está escribiendo en {language}")

    # dunder methods
    def __eq__(self, other):
        return self.name == other.name and self.age == other.age

    # cuando un método no quieres que este unido a una instance class
    # sino a toda la clase se puede usar @staticmethod
    @staticmethod
    def entry_salary(age):
        if age < 25:
            return 2000
        if age < 30:
            return 4000
        return 7000



se1 = SoftwareEngineer("Pedro", 28, "junior", 3400)
se2 = SoftwareEngineer("Pedro", 28, "junior", 3400)

print(se1.alias, se1.age)
print(SoftwareEngineer.alias)
se1.code()
se1.code_in_language("python")

print(se1 == se2)
print(se1.entry_salary(23))
print(SoftwareEngineer.entry_salary(40))