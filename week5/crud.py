from sqlalchemy.orm import Session
import models, schemas

# READ
def get_user(db: Session, user_id: int):
    return db.query(models.User).filter(models.User.id == user_id).first()

# READ
def get_users(db: Session):
    return db.query(models.User).all()

# CREATE
def create_user(db: Session, user: schemas.UserCreate):
    db_user = models.User(name=user.name, age=user.age, role=user.role)
    db.add(db_user)
    db.commit()
    db.refresh(db_user)
    return db_user

# UPDATE
def update_user(db: Session, user_id: int, user: schemas.UserUpdate):
    db_user = get_user(db, user_id)
    db_user.name = user.name
    db_user.age = user.age
    db.commit()
    db.refresh(db_user)
    return db_user

# DELETE
def delete_user(db: Session, user_id: int):
    db_user = get_user(db, user_id)
    db.delete(db_user)
    db.commit()
    return db_user