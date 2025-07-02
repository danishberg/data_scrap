from sqlalchemy import create_engine, Column, Integer, String, Text, Float, DateTime, Boolean, ForeignKey, Table
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
from datetime import datetime
import json

Base = declarative_base()

# Association table for many-to-many relationship between centers and materials
center_materials = Table(
    'center_materials',
    Base.metadata,
    Column('center_id', Integer, ForeignKey('scrap_centers.id')),
    Column('material_id', Integer, ForeignKey('materials.id'))
)

class ScrapCenter(Base):
    __tablename__ = 'scrap_centers'
    
    id = Column(Integer, primary_key=True)
    name = Column(String(255), nullable=False)
    website = Column(String(500))
    
    # Address information
    full_address = Column(Text)
    street_address = Column(String(255))
    city = Column(String(100))
    state_region = Column(String(100))
    postal_code = Column(String(20))
    country = Column(String(50))
    
    # Coordinates
    latitude = Column(Float)
    longitude = Column(Float)
    
    # Contact information
    phone_primary = Column(String(50))
    phone_secondary = Column(String(50))
    email_primary = Column(String(100))
    email_secondary = Column(String(100))
    
    # Social media and messaging
    facebook_url = Column(String(500))
    twitter_url = Column(String(500))
    instagram_url = Column(String(500))
    linkedin_url = Column(String(500))
    whatsapp_number = Column(String(50))
    telegram_contact = Column(String(100))
    
    # Business information
    working_hours = Column(Text)  # JSON string
    description = Column(Text)
    services_offered = Column(Text)  # JSON string
    
    # Scraping metadata
    source_url = Column(String(500))
    scraped_at = Column(DateTime, default=datetime.utcnow)
    last_updated = Column(DateTime, default=datetime.utcnow)
    verification_status = Column(String(20), default='pending')  # pending, verified, invalid
    
    # Relationships
    materials = relationship("Material", secondary=center_materials, back_populates="centers")
    prices = relationship("MaterialPrice", back_populates="center")
    
    def to_dict(self):
        return {
            'id': self.id,
            'name': self.name,
            'website': self.website,
            'full_address': self.full_address,
            'street_address': self.street_address,
            'city': self.city,
            'state_region': self.state_region,
            'postal_code': self.postal_code,
            'country': self.country,
            'latitude': self.latitude,
            'longitude': self.longitude,
            'phone_primary': self.phone_primary,
            'phone_secondary': self.phone_secondary,
            'email_primary': self.email_primary,
            'email_secondary': self.email_secondary,
            'facebook_url': self.facebook_url,
            'twitter_url': self.twitter_url,
            'instagram_url': self.instagram_url,
            'linkedin_url': self.linkedin_url,
            'whatsapp_number': self.whatsapp_number,
            'telegram_contact': self.telegram_contact,
            'working_hours': json.loads(self.working_hours) if self.working_hours else {},
            'description': self.description,
            'services_offered': json.loads(self.services_offered) if self.services_offered else [],
            'source_url': self.source_url,
            'scraped_at': self.scraped_at.isoformat() if self.scraped_at else None,
            'last_updated': self.last_updated.isoformat() if self.last_updated else None,
            'verification_status': self.verification_status,
            'materials': [material.name for material in self.materials],
            'prices': [price.to_dict() for price in self.prices]
        }

class Material(Base):
    __tablename__ = 'materials'
    
    id = Column(Integer, primary_key=True)
    name = Column(String(100), unique=True, nullable=False)
    category = Column(String(50))  # ferrous, non-ferrous, electronic, etc.
    description = Column(Text)
    
    # Relationships
    centers = relationship("ScrapCenter", secondary=center_materials, back_populates="materials")
    prices = relationship("MaterialPrice", back_populates="material")

class MaterialPrice(Base):
    __tablename__ = 'material_prices'
    
    id = Column(Integer, primary_key=True)
    center_id = Column(Integer, ForeignKey('scrap_centers.id'))
    material_id = Column(Integer, ForeignKey('materials.id'))
    
    price_per_unit = Column(Float)
    unit = Column(String(20))  # lb, kg, ton, etc.
    currency = Column(String(10), default='USD')
    
    # Price metadata
    last_updated = Column(DateTime, default=datetime.utcnow)
    is_current = Column(Boolean, default=True)
    source_url = Column(String(500))
    notes = Column(Text)
    
    # Relationships
    center = relationship("ScrapCenter", back_populates="prices")
    material = relationship("Material", back_populates="prices")
    
    def to_dict(self):
        return {
            'material_name': self.material.name if self.material else None,
            'price_per_unit': self.price_per_unit,
            'unit': self.unit,
            'currency': self.currency,
            'last_updated': self.last_updated.isoformat() if self.last_updated else None,
            'notes': self.notes
        }

class DatabaseManager:
    def __init__(self, database_url):
        self.engine = create_engine(database_url)
        Base.metadata.create_all(self.engine)
        Session = sessionmaker(bind=self.engine)
        self.session = Session()
    
    def add_scrap_center(self, center_data):
        """Add a new scrap center to the database"""
        try:
            center = ScrapCenter(**center_data)
            self.session.add(center)
            self.session.commit()
            return center
        except Exception as e:
            self.session.rollback()
            print(f"Error adding scrap center: {e}")
            return None
    
    def get_or_create_material(self, material_name, category=None):
        """Get existing material or create new one"""
        material = self.session.query(Material).filter_by(name=material_name).first()
        if not material:
            material = Material(name=material_name, category=category)
            self.session.add(material)
            self.session.commit()
        return material
    
    def add_material_price(self, center_id, material_name, price_data):
        """Add material price information"""
        try:
            material = self.get_or_create_material(material_name)
            price = MaterialPrice(
                center_id=center_id,
                material_id=material.id,
                **price_data
            )
            self.session.add(price)
            self.session.commit()
            return price
        except Exception as e:
            self.session.rollback()
            print(f"Error adding material price: {e}")
            return None
    
    def get_all_centers(self):
        """Get all scrap centers"""
        return self.session.query(ScrapCenter).all()
    
    def close(self):
        """Close database session"""
        self.session.close() 