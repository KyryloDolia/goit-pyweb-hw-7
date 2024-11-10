import random

from faker import Faker
from sqlalchemy.exc import SQLAlchemyError

from conf.db import session
from conf.models import Teacher, Student, Group, Subject, Grade


fake = Faker('uk-UA')


def insert_students():
    groups = session.query(Group).all()
    for _ in range(10):
        student = Student(
            fullname=fake.full_name(),
            group_id=random.choice(groups).id
        )
        session.add(student)


def insert_teachers():
    for _ in range(6):
        teacher = Teacher(
            fullname=fake.full_name()
        )
        session.add(teacher)


def insert_groups():
    for _ in range(3):
        group = Group(
            name=fake.random_uppercase_letter()
        )
        session.add(group)


def insert_subjects():
    teachers = session.query(Teacher).all()
    for _ in range(6):
        subj = Subject(
            name=fake.word(),
            teacher_id=random.choice(teachers).id
        )
        session.add(subj)


def insert_grades():
    students = session.query(Student).all()
    subjects = session.query(Subject).all()
    for _ in range(10):
        grd = Grade(
            grade=round(random.randint(1, 100)),
            grade_date=fake.date_this_year(),
            student_id = random.choice(students).id,
            subject_id = random.choice(subjects).id
        )
        session.add(grd)


if __name__ == '__main__':
    try:
        insert_groups()
        insert_teachers()
        insert_subjects()
        insert_students()
        insert_grades()
        session.commit()
    except SQLAlchemyError as e:
        print(e)
        session.rollback()
    finally:
        session.close()
