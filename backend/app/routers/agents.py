from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, text
from sqlalchemy.orm import selectinload
from typing import List, Optional

from app.database import get_db
from app.models.agent import Agent
from app.models.agent_list import AgentList
from app.models.user import User
from app.schemas.agent import AgentResponse, AgentCreate, AgentUpdate
from app.services.auth import get_current_active_user

router = APIRouter()


def _build_agent_response(agent: Agent, agent_list_data: Optional[AgentList] = None) -> dict:
    """Build agent response dict with agent_list data if available."""
    response = {
        'id': agent.id,
        'name': agent.name,
        'mobile': agent.mobile,
        'cea': agent.cea,
        'rating': agent.rating,
        'description': agent.description,
        'url': agent.url,
        'source_id': agent.source_id,
        'created_at': agent.created_at,
        'updated_at': agent.updated_at,
        # Default values for agent_list fields
        'company_name': None,
        'agency_license': None,
        'license_expiry': None,
        'registration_date': None,
        'photo_url': None,
    }
    
    # Enrich with agent_list data if available
    if agent_list_data:
        response['company_name'] = agent_list_data.company_name
        response['agency_license'] = agent_list_data.agency_license
        response['license_expiry'] = agent_list_data.license_expiry
        response['registration_date'] = agent_list_data.registration_date
        response['photo_url'] = agent_list_data.photo_url
    
    return response


@router.get("/me", response_model=Optional[AgentResponse])
async def read_my_agent_profile(
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db)
):
    """
    Get current user's agent profile with enriched data from agent_list.
    """
    if current_user.role != "agent" and current_user.role != "admin":
         raise HTTPException(status_code=403, detail="Not authorized")
         
    # Fetch agent linked to this user
    result = await db.execute(select(Agent).where(Agent.user_id == current_user.id))
    agent = result.scalar_one_or_none()
    
    if not agent:
        return None
    
    # Try to find matching agent_list record via CEA
    agent_list_data = None
    if agent.cea:
        result = await db.execute(
            select(AgentList).where(AgentList.cea_number == agent.cea)
        )
        agent_list_data = result.scalar_one_or_none()
    
    return _build_agent_response(agent, agent_list_data)


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
        name=current_user.full_name,
        mobile=agent_in.mobile,
        cea=agent_in.cea,
        description=agent_in.description,
        url=agent_in.url,
        user_id=current_user.id
    )
    db.add(agent)
    await db.commit()
    await db.refresh(agent)
    
    # Try to find matching agent_list record
    agent_list_data = None
    if agent.cea:
        result = await db.execute(
            select(AgentList).where(AgentList.cea_number == agent.cea)
        )
        agent_list_data = result.scalar_one_or_none()
    
    return _build_agent_response(agent, agent_list_data)


@router.get("/{agent_id}", response_model=AgentResponse)
async def get_agent(
    agent_id: int,
    db: AsyncSession = Depends(get_db)
):
    """
    Get agent by ID with enriched data from agent_list.
    """
    stmt = select(Agent).where(Agent.id == agent_id)
    result = await db.execute(stmt)
    agent = result.scalar_one_or_none()
    
    if not agent:
        raise HTTPException(status_code=404, detail="Agent not found")
    
    # Try to find matching agent_list record via CEA
    agent_list_data = None
    if agent.cea:
        result = await db.execute(
            select(AgentList).where(AgentList.cea_number == agent.cea)
        )
        agent_list_data = result.scalar_one_or_none()
    
    return _build_agent_response(agent, agent_list_data)


@router.get("/", response_model=List[AgentResponse])
async def list_agents(
    skip: int = Query(0, ge=0),
    limit: int = Query(20, ge=1, le=100),
    db: AsyncSession = Depends(get_db)
):
    """
    List agents with enriched data from agent_list.
    """
    stmt = select(Agent).offset(skip).limit(limit)
    result = await db.execute(stmt)
    agents = result.scalars().all()
    
    # Build response with agent_list data
    responses = []
    for agent in agents:
        agent_list_data = None
        if agent.cea:
            result = await db.execute(
                select(AgentList).where(AgentList.cea_number == agent.cea)
            )
            agent_list_data = result.scalar_one_or_none()
        responses.append(_build_agent_response(agent, agent_list_data))
    
    return responses
