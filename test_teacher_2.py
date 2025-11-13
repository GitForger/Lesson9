import time
import pytest
from sqlalchemy import Column, Integer, String, create_engine, text
from sqlalchemy.orm import declarative_base, sessionmaker

Base = declarative_base()


class Teacher(Base):
    __tablename__ = "teacher"
    __table_args__ = {"extend_existing": True}
    teacher_id = Column(Integer, primary_key=True)
    email = Column(String(100))
    group_id = Column(Integer)


@pytest.fixture(scope="function")
def db_session():
    engine = create_engine(
        "postgresql://postgres:admin123@localhost:5432/postgres")
    connection = engine.connect()
    transaction = connection.begin()
    Session = sessionmaker(bind=connection)
    session = Session()

    yield session

    session.close()
    transaction.rollback()
    connection.close()
    engine.dispose()


def test_create_teacher(db_session):
    initial_count = db_session.query(Teacher).count()

    test_id = int(time.time() % 100000)
    test_email = f"proof_{test_id}@example.com"

    teacher = Teacher(teacher_id=test_id, email=test_email, group_id=999)
    db_session.add(teacher)
    db_session.commit()

    teacher_from_db = db_session.get(Teacher, test_id)
    assert teacher_from_db is not None

    result = db_session.execute(
        text("SELECT email FROM teacher WHERE teacher_id = :id"),
        {"id": test_id},
    )
    email_from_sql = result.scalar()
    assert email_from_sql == test_email

    new_count = db_session.query(Teacher).count()
    assert new_count == initial_count + 1


def test_update_teacher(db_session):
    test_id = int(time.time() % 100000) + 1
    original_email = f"before_{test_id}@example.com"
    updated_email = f"after_{test_id}@example.com"

    teacher = Teacher(teacher_id=test_id, email=original_email, group_id=100)
    db_session.add(teacher)
    db_session.commit()

    teacher.email = updated_email
    db_session.commit()

    updated_teacher = db_session.get(Teacher, test_id)
    assert updated_teacher.email == updated_email

    result = db_session.execute(
        text("SELECT email FROM teacher WHERE teacher_id = :id"),
        {"id": test_id},
    )
    assert result.scalar() == updated_email


def test_delete_teacher(db_session):
    test_id = int(time.time() % 100000) + 2
    test_email = f"delete_{test_id}@example.com"

    teacher = Teacher(teacher_id=test_id, email=test_email, group_id=200)
    db_session.add(teacher)
    db_session.commit()

    initial_count = db_session.query(Teacher).count()

    db_session.delete(teacher)
    db_session.commit()

    teacher_after = db_session.get(Teacher, test_id)
    assert teacher_after is None

    final_count = db_session.query(Teacher).count()
    assert final_count == initial_count - 1


def test_data_not_persisted(db_session):
    initial_count = db_session.query(Teacher).count()

    test_ids = [99991, 99992, 99993]
    for test_id in test_ids:
        teacher = Teacher(
            teacher_id=test_id,
            email=f"temp_{test_id}@example.com",
            group_id=test_id,
        )
        db_session.add(teacher)

    db_session.commit()

    after_add_count = db_session.query(Teacher).count()
    assert after_add_count == initial_count + 3


def test_final_verification():
    engine = create_engine(
        "postgresql://postgres:admin123@localhost:5432/postgres")
    Session = sessionmaker(bind=engine)
    session = Session()

    test_ids = [99991, 99992, 99993]
    for test_id in test_ids:
        teacher = session.get(Teacher, test_id)
        assert teacher is None

    session.close()


def test_modern_select_methods(db_session):
    stmt = text("SELECT COUNT(*) FROM teacher WHERE group_id IS NOT NULL")
    result = db_session.execute(stmt)
    count_with_group = result.scalar()
    assert count_with_group >= 0

    teachers = db_session.query(
        Teacher).where(Teacher.group_id.isnot(None)).limit(3).all()
    assert len(teachers) <= 3


if __name__ == "__main__":
    engine = create_engine(
        "postgresql://postgres:admin123@localhost:5432/postgres")
    Session = sessionmaker(bind=engine)
    session = Session()

    test_id = 88888
    teacher = Teacher(
        teacher_id=test_id,
        email="manual_test@example.com",
        group_id=888,
    )
    session.add(teacher)
    session.commit()

    found = session.get(Teacher, test_id)
    assert found is not None

    session.delete(teacher)
    session.commit()

    found_after = session.get(Teacher, test_id)
    assert found_after is None
    session.close()

