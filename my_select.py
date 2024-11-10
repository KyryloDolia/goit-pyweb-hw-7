
from sqlalchemy import func, desc, select, and_

from conf.models import Grade, Student, Group, Subject
from conf.db import session


def select_01():
    """
    SELECT
        s.id,
        s.fullname,
        ROUND(AVG(g.grade), 2) AS average_grade
    FROM students s
    JOIN grades g ON s.id = g.student_id
    GROUP BY s.id
    ORDER BY average_grade DESC
    LIMIT 5;
    """
    result = session.query(Student.id, Student.fullname, func.round(func.avg(Grade.grade), 2).label('average_grade')) \
        .select_from(Student).join(Grade).group_by(Student.id).order_by(desc('average_grade')).limit(5).all()
    return result


def select_02():
    """
    SELECT
        s.id,
        s.fullname,
        ROUND(AVG(g.grade), 2) AS average_grade
    FROM grades g
    JOIN students s ON s.id = g.student_id
    where g.subject_id = 1
    GROUP BY s.id
    ORDER BY average_grade DESC
    LIMIT 1;
    """
    result = session.query(Student.id, Student.fullname, func.round(func.avg(Grade.grade), 2).label('average_grade')) \
        .select_from(Grade).join(Student).filter(Grade.subject_id == 1).group_by(Student.id).order_by(
        desc('average_grade')).limit(1).all()
    return result


def select_03():
    """
    SELECT
        g.name AS group_name,
        AVG(gr.grade) AS average_grade
    FROM grades gr
    JOIN students s ON gr.student_id = s.id
    JOIN groups g ON s.group_id = g.id
    WHERE gr.subject_id = 1
    GROUP BY g.name;
    """
    result = (
        session.query(
            Group.name.label('group_name'),
            func.avg(Grade.grade).label('average_grade')
        )
        .join(Student, Grade.student_id == Student.id)
        .join(Group, Student.group_id == Group.id)
        .filter(Grade.subject_id == 1)
        .group_by(Group.name)
        .all()
    )

    return result


def select_04():
    """
    SELECT AVG(grade) AS average_grade
    FROM grades;
    """
    result = session.query(func.avg(Grade.grade)).scalar()
    return result


def select_05():
    """
    SELECT name AS course_name
    FROM subjects
    WHERE teacher_id = 1;
    """
    result = session.query(Subject.name).filter(Subject.teacher_id == 1).all()
    return result


def select_06():
    """
    SELECT fullname AS student_name
    FROM students
    WHERE group_id = 1;
    """
    result = session.query(Student.fullname).filter(Student.group_id == 1).all()
    return result

def select_07():
    """
    SELECT
        s.fullname AS student_name,
        gr.grade,
        gr.subject_id
    FROM grades gr
    JOIN students s ON gr.student_id = s.id
    WHERE
        s.group_id = 1
        AND gr.subject_id = 1;
    """
    result = (
        session.query(
            Student.fullname.label('student_name'),
            Grade.grade,
            Grade.subject_id
        )
        .join(Student, Grade.student_id == Student.id)
        .filter(Student.group_id == 1, Grade.subject_id == 1)
        .all()
    )
    return result


def select_08():
    """
    SELECT
        AVG(gr.grade) AS average_grade
    FROM
        grades gr
    JOIN
        subjects sub ON gr.subject_id = sub.id
    WHERE
        sub.teacher_id = 1;
    """
    result = (
        session.query(
            func.avg(Grade.grade).label('average_grade')
        )
        .join(Subject, Grade.subject_id == Subject.id)
        .filter(Subject.teacher_id == 1)
        .scalar()
    )
    return result


def select_09():
    """
    SELECT sub.name AS course_name
    FROM grades gr
    JOIN subjects sub ON gr.subject_id = sub.id
    WHERE gr.student_id = 1
    GROUP BY sub.name;
    """
    result = (
        session.query(Subject.name.label('course_name'))
        .join(Grade, Grade.subject_id == Subject.id)
        .filter(Grade.student_id == 1)
        .group_by(Subject.name)
        .all()
    )
    return result


def select_10():
    """
    SELECT
        sub.name AS course_name
    FROM
        grades gr
    JOIN
        subjects sub ON gr.subject_id = sub.id
    WHERE
        gr.student_id = 1
        AND sub.teacher_id = 1
    GROUP BY
        sub.name;
    """
    result = (
        session.query(Subject.name.label("course_name"))
        .join(Grade, Grade.subject_id == Subject.id)
        .filter(Grade.student_id == 1, Subject.teacher_id == 1)
        .group_by(Subject.name)
        .all()
    )
    return result


def select_11():
    """
     SELECT
        AVG(gr.grade) AS average_grade
    FROM
        grades gr
    JOIN
        subjects sub ON gr.subject_id = sub.id
    WHERE
        gr.student_id = 1
        AND sub.teacher_id = 1;
    """
    result = (
        session.query(func.avg(Grade.grade).label("average_grade"))
        .join(Subject, Grade.subject_id == Subject.id)
        .filter(Grade.student_id == 1, Subject.teacher_id == 1)
        .scalar()
    )
    return result


def select_12():
    """
    select max(grade_date)
    from grades g
    join students s on s.id = g.student_id
    where g.subject_id = 2 and s.group_id  =3;

    select s.id, s.fullname, g.grade, g.grade_date
    from grades g
    join students s on g.student_id = s.id
    where g.subject_id = 2 and s.group_id = 3 and g.grade_date = (
        select max(grade_date)
        from grades g2
        join students s2 on s2.id=g2.student_id
        where g2.subject_id = 2 and s2.group_id = 3
    );
    :return:
    """

    subquery = (select(func.max(Grade.grade_date)).join(Student).filter(and_(
        Grade.subject_id == 2, Student.group_id == 3
    ))).scalar_subquery()

    result = session.query(Student.id, Student.fullname, Grade.grade, Grade.grade_date) \
        .select_from(Grade) \
        .join(Student) \
        .filter(and_(Grade.subject_id == 2, Student.group_id == 3, Grade.grade_date == subquery)).all()

    return result


if __name__ == '__main__':
    print(select_01())
    print(select_02())
    print(select_03())
    print(select_04())
    print(select_05())
    print(select_06())
    print(select_07())
    print(select_08())
    print(select_09())
    print(select_10())
    print(select_11())
    print(select_12())
