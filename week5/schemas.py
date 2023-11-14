from pydantic import BaseModel

class UserBase(BaseModel):
  id: int

class UserCreate(BaseModel):
  name: str
  age: int
  role: str

class UserUpdate(BaseModel):
  name: str
  age: int

class User(UserBase):
  name: str
  age: int
  role: str

  class Config:
    orm_mode = True