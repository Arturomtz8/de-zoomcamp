class Employee:
    def __init__(self, name, age, salary):
        self.name = name
        self.age = age
        self.salary = salary

    def work(self):
        print(f"{self.name} is working")


class SoftwareEngineer(Employee):
    def __init__(self, name, age, salary, level):
        super().__init__(name, age, salary)
        self.level = level

    def work(self):
        print(f"{self.name} is coding")


class Designer(Employee):
    def work(self):
        print(f"{self.name} is designing")


se1 = SoftwareEngineer("PEDRO", 28, 2300, "jr")
print(se1.age)
print(se1.level)
se1.work()
de = Designer("Juana", 19, 3232323)
de.work()
