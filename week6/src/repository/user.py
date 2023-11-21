from typing import List, Optional
from fastapi import Depends
from sqlalchemy.orm import Session
import uuid

from config.database import get_db
from models.user import User as UserModel
from schemas.user import User, UserCreate

class UserRepository():
    def __init__(self, db: Session = Depends(get_db)) -> None :
        self.db = db
    
    def get_all_users(self) -> List[User]:
        return self.db.query(UserModel).all()
    
    def get_count_by_user(self) -> int:
        return self.db.query(UserModel).count()
    
    def get_user_by_id(self, user_id: str) -> User:
        return self.db.query(UserModel).filter(UserModel.user_id == user_id).first()
    
    def get_user_by_name(self, user_name: str) -> User:
        return self.db.query(UserModel).filter(UserModel.user_name == user_name).first()
    
    def create_user(self, user_create_dto: UserCreate, commit: bool = True) -> User:
        user_name = user_create_dto.user_name
        age = user_create_dto.age
        
        exists = self.get_user_by_name(user_name=user_name)
        if exists:
            return exists
        else:
            user_id = str(uuid.uuid4())[:6]
            data = UserModel(
                user_id=user_id,
                user_name=user_name,
                age=age,
            )
            self.db.add(data)
            if commit:
                self.db.commit()
                self.db.refresh(data)
            return data