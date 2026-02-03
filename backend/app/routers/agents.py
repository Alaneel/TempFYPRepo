from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from sqlalchemy.orm import selectinload
from typing import List, Optional

from app.database import get_db
from app.models.agent import Agent
from app.models.user import User
from app.schemas.agent import AgentResponse, AgentCreate, AgentUpdate
from app.services.auth import get_current_active_user

router = APIRouter()

@router.get("/me", response_model=Optional[AgentResponse])
async def read_my_agent_profile(
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db)
):
    """
    Get current user's agent profile.
    """
    if current_user.role != "agent" and current_user.role != "admin": # Allow admin too for testing
         raise HTTPException(status_code=403, detail="Not authorized")
         
    # Fetch agent linked to this user
    result = await db.execute(select(Agent).where(Agent.user_id == current_user.id))
    agent = result.scalar_one_or_none()
    
    return agent # Returns null if no profile yet


@router.post("/", response_model=AgentResponse)
async def create_agent_profile(
    agent_in: AgentCreate,
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db)
):
    """
    Create or update agent profile for the current user.
    """
    if current_user.role != "agent" and current_user.role != "admin":
         raise HTTPException(status_code=403, detail="Not authorized")
         
    # Check if already exists
    result = await db.execute(select(Agent).where(Agent.user_id == current_user.id))
    existing_agent = result.scalar_one_or_none()
    
    if existing_agent:
        raise HTTPException(status_code=400, detail="Agent profile already exists")
    
    # Create
    agent = Agent(
        name=current_user.full_name, # Default to user name
        mobile=agent_in.mobile,
        cea=agent_in.cea,
        description=agent_in.description,
        url=agent_in.url,
        user_id=current_user.id
    )
    db.add(agent)
    await db.commit()
    await db.refresh(agent)
    return agent

@router.get("/{agent_id}", response_model=AgentResponse)
async def get_agent(
    agent_id: int,
    db: AsyncSession = Depends(get_db)
):
    stmt = select(Agent).where(Agent.id == agent_id)
    result = await db.execute(stmt)
    agent = result.scalar_one_or_none()
    
    if not agent:
        raise HTTPException(status_code=404, detail="Agent not found")
        
    return agent

