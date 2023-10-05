from chalice import Chalice, Response
from sqlalchemy import create_engine, Column, String, Integer, Float, ForeignKey
from sqlalchemy.orm import Session, relationship
from sqlalchemy.ext.declarative import declarative_base
from marshmallow import Schema, fields
import os

app = Chalice(app_name='ProductService')

if os.environ.get("DEBUG", False):
    app.debug = True

db_user = os.environ.get("DB_USER")
db_password = os.environ.get("DB_PASSWORD")
db_host = os.environ.get("DB_HOST")
db_port = os.environ.get("DB_PORT")
db_name = os.environ.get("DB_NAME")

engine = create_engine(f"postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}", echo=True)

Base = declarative_base()


class UnitMeasure(Base):
    __tablename__ = "unit_measures"

    id = Column(Integer, primary_key=True, nullable=False)
    name = Column(String, nullable=False, unique=True)


class Product(Base):
    __tablename__ = "products"

    id = Column(Integer, primary_key=True, nullable=False)
    name = Column(String, nullable=False, unique=True)
    price = Column(Float, nullable=False)
    unit_measure_id = Column(Integer, ForeignKey('unit_measures.id', ondelete='NO ACTION'), nullable=False)

    unit_measures = relationship(UnitMeasure)


class UnitMeasureSchema(Schema):
    id = fields.Integer()
    name = fields.String()


class ProductSchema(Schema):
    id = fields.Integer()
    name = fields.String()
    price = fields.Float()
    unit_measure_id = fields.Integer()
    unit_measures = fields.Nested(UnitMeasureSchema)


Base.metadata.create_all(engine)


messages = {
    "RecordFound": "The {resource} has been found successfully",
    "RecordCreated": "The {resource} has been created successfully",
    "RecordUpdated": "The {resource} with id '{id}' has been updated successfully",
    "RecordDeleted": "The {resource} with id '{id}'has been deleted successfully",
    "RecordNotFound": "The {resource} with id '{id}' has not been found",
    "RecordAlreadyExists": "The {resource} already exists",
    "InternalError": "An error occurred during your request, please try again",
}


@app.route('/products', methods=['GET'])
def indexProducts():
    try:
        offset = app.current_request.query_params.get("offset", 0)
        limit = app.current_request.query_params.get("limit", 10)
        search = app.current_request.query_params.get("search", "")
        products = []
        totalRecords = 0

        with Session(engine) as session:
            for data in session.query(Product).filter(Product.name.like(f"%{search}%")).offset(offset).limit(limit).all():
                products.append(ProductSchema().dump(data))
                totalRecords = session.query(Product).filter(Product.name.like(f"%{search}%")).count()
        return MakeResponsePaginate(
            message=messages.get("RecordFound").format(resource="products"),
            data=products,
            totalRecords=totalRecords
        )
    except KeyError as e:
        return e


@app.route('/product/{id}', methods=['GET'])
def showProduct(id):
    try:
        with Session(engine) as session:
            data = session.query(Product).where(Product.id == id).first()
            if data is None:
                return MakeResponse(
                    message=messages.get("RecordNotFound").format(resource="product", id=id),
                    status_code=400
                )
            product = ProductSchema().dump(data)

        return MakeResponse(
            message=messages.get("RecordFound").format(resource="product"),
            data=product
        )
    except KeyError as e:
        return e


@app.route('/product', methods=['POST'])
def storeProduct():
    try:
        json_body = app.current_request.json_body
        product = Product(
            name=json_body.get("name"),
            price=json_body.get("price"),
            unit_measure_id=json_body.get("unit_measure_id")
        )

        with Session(engine) as session:
            session.add(product)
            session.flush()
            product = ProductSchema().dump(product)
            session.commit()

        return MakeResponse(
            message=messages.get("RecordCreated").format(resource="product"),
            data=product
        )
    except KeyError as e:
        return e


@app.route('/product/{id}', methods=['PUT'])
def updateProduct(id):
    try:
        json_body = app.current_request.json_body

        with Session(engine) as session:
            product = session.query(Product).where(Product.id == id).first()
            if product is None:
                return MakeResponse(
                    message=messages.get("RecordNotFound").format(resource="product", id=id),
                    status_code=400
                )
            product.name = json_body.get("name")
            product.price = json_body.get("price")
            product.unit_measure_id = json_body.get("unit_measure_id")
            product = ProductSchema().dump(product)
            session.query(Product).where(Product.id == id).update(product)
            session.commit()

        return MakeResponse(
            message=messages.get("RecordUpdated").format(resource="product", id=id),
            data=product
        )
    except KeyError as e:
        return e


@app.route('/product/{id}', methods=['DELETE'])
def destroyProduct(id):
    try:
        with Session(engine) as session:
            product = session.query(Product).where(Product.id == id).first()
            if product is None:
                return MakeResponse(
                    message=messages.get("RecordNotFound").format(resource="product", id=id),
                    status_code=400
                )
            product = session.query(Product).where(Product.id == id).delete()
            session.commit()

        return MakeResponse(
            message=messages.get("RecordDeleted").format(resource="product", id=id),
        )
    except KeyError as e:
        return e


def MakeResponse(message, data=None, status_code=200, error=None):
    status = "OK"
    if status_code == 400:
        status = "BadRequest"
    elif status_code == 500:
        status = "InternalServerError"

    return Response(body={
        "status": status,
        "code": status_code,
        "message": message,
        "error": error,
        "data": data,
    })


def MakeResponsePaginate(message, data, totalRecords):
    return Response(body={
        "status": "OK",
        "code": 200,
        "message": message,
        "error": None,
        "data": data,
        "total_records": totalRecords,
    })
