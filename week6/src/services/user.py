from fastapi import Depends

from schemas.user import User, UserAll, UserCreate
from repository.user import UserRepository

class UserService():
    def __init__(self, repository: UserRepository = Depends()) -> None:
        self.repository = repository
    
    def get_all_users(self) -> UserAll:
        users = self.repository.get_all_users()
        total = self.repository.get_count_by_user()
        return {
            "total": total,
            "users": users
        } 
    
    def get_user_by_id(self, user_id: str) -> User:
        return self.repository.get_user_by_id(user_id)
    
    def create_user(self, user_create_dto: UserCreate) -> User:
        return self.repository.create_user(user_create_dto)
    