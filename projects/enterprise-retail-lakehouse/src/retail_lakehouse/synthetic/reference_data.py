"""Small, hand-written reference lists (no Faker): Brazilian geography, names, catalogue.

Brands, suppliers and carriers are fictitious. Customer e-mails use reserved domains
(RFC 2606). No CPF/CNPJ or phone numbers are generated: synthetic numbers with valid check
digits could collide with real people or companies.
"""

from dataclasses import dataclass
from typing import Final


@dataclass(frozen=True, slots=True)
class City:
    name: str
    state: str
    region: str
    weight: float  # relative share of customers/demand


CITIES: Final = (
    City("São Paulo", "SP", "Sudeste", 12.0),
    City("Rio de Janeiro", "RJ", "Sudeste", 7.0),
    City("Belo Horizonte", "MG", "Sudeste", 3.0),
    City("Brasília", "DF", "Centro-Oeste", 3.0),
    City("Salvador", "BA", "Nordeste", 3.0),
    City("Fortaleza", "CE", "Nordeste", 2.8),
    City("Curitiba", "PR", "Sul", 2.2),
    City("Manaus", "AM", "Norte", 2.2),
    City("Recife", "PE", "Nordeste", 2.0),
    City("Porto Alegre", "RS", "Sul", 2.0),
    City("Belém", "PA", "Norte", 1.6),
    City("Goiânia", "GO", "Centro-Oeste", 1.6),
    City("Guarulhos", "SP", "Sudeste", 1.4),
    City("Campinas", "SP", "Sudeste", 1.3),
    City("São Luís", "MA", "Nordeste", 1.1),
    City("Maceió", "AL", "Nordeste", 1.0),
    City("Natal", "RN", "Nordeste", 0.9),
    City("Teresina", "PI", "Nordeste", 0.9),
    City("Campo Grande", "MS", "Centro-Oeste", 0.9),
    City("João Pessoa", "PB", "Nordeste", 0.8),
    City("Ribeirão Preto", "SP", "Sudeste", 0.7),
    City("Uberlândia", "MG", "Sudeste", 0.7),
    City("Cuiabá", "MT", "Centro-Oeste", 0.6),
    City("Aracaju", "SE", "Nordeste", 0.6),
    City("Joinville", "SC", "Sul", 0.6),
    City("Londrina", "PR", "Sul", 0.6),
    City("Florianópolis", "SC", "Sul", 0.5),
    City("Porto Velho", "RO", "Norte", 0.5),
    City("Vitória", "ES", "Sudeste", 0.4),
    City("Macapá", "AP", "Norte", 0.4),
)

FIRST_NAMES: Final = (
    "Ana",
    "Maria",
    "Julia",
    "Beatriz",
    "Mariana",
    "Larissa",
    "Camila",
    "Fernanda",
    "Gabriela",
    "Leticia",
    "Amanda",
    "Bruna",
    "Carolina",
    "Patricia",
    "Aline",
    "Juliana",
    "Helena",
    "Alice",
    "Laura",
    "Valentina",
    "João",
    "José",
    "Pedro",
    "Lucas",
    "Gabriel",
    "Matheus",
    "Rafael",
    "Gustavo",
    "Felipe",
    "Bruno",
    "Thiago",
    "Rodrigo",
    "Diego",
    "Carlos",
    "Paulo",
    "Marcos",
    "André",
    "Leonardo",
    "Eduardo",
    "Ricardo",
)

LAST_NAMES: Final = (
    "Silva",
    "Santos",
    "Oliveira",
    "Souza",
    "Rodrigues",
    "Ferreira",
    "Alves",
    "Pereira",
    "Lima",
    "Gomes",
    "Costa",
    "Ribeiro",
    "Martins",
    "Carvalho",
    "Almeida",
    "Lopes",
    "Soares",
    "Fernandes",
    "Vieira",
    "Barbosa",
    "Rocha",
    "Dias",
    "Nascimento",
    "Andrade",
    "Moreira",
    "Nunes",
    "Marques",
    "Machado",
    "Mendes",
    "Freitas",
    "Cardoso",
    "Ramos",
    "Gonçalves",
    "Santana",
    "Teixeira",
    "Araújo",
    "Pinto",
    "Correia",
    "Batista",
    "Monteiro",
)

CUSTOMER_EMAIL_DOMAINS: Final = ("example.com", "example.org", "example.net")
SUPPLIER_SUFFIXES: Final = ("Distribuidora", "Indústria", "Comércio", "Atacadista", "Importadora")

BRANDS: Final = (
    "Aurora",
    "Boreal",
    "Cajuí",
    "Dunas",
    "Estrela Guia",
    "Flamboyant",
    "Girassol",
    "Horizonte",
    "Ipê",
    "Jacarandá",
    "Lumen",
    "Maresia",
    "Nativa",
    "Orla",
    "Pitanga",
    "Quaresma",
    "Rio Claro",
    "Serra Azul",
    "Tropicália",
    "Vereda",
)


@dataclass(frozen=True, slots=True)
class Subcategory:
    name: str
    min_price: float
    max_price: float


@dataclass(frozen=True, slots=True)
class Department:
    name: str
    product_share: float  # relative share of the catalogue
    multi_unit: bool  # groceries are bought several units at a time
    subcategories: tuple[Subcategory, ...]


DEPARTMENTS: Final = (
    Department(
        "Eletrônicos",
        1.0,
        False,
        (
            Subcategory("Smartphones", 800, 4500),
            Subcategory("Notebooks", 2500, 8000),
            Subcategory("Fones de Ouvido", 80, 1200),
            Subcategory("Smart TVs", 1500, 6000),
            Subcategory("Acessórios de Informática", 30, 400),
        ),
    ),
    Department(
        "Eletrodomésticos",
        0.7,
        False,
        (
            Subcategory("Geladeiras", 2000, 6000),
            Subcategory("Micro-ondas", 400, 1200),
            Subcategory("Liquidificadores", 100, 400),
            Subcategory("Ventiladores", 120, 500),
        ),
    ),
    Department(
        "Moda",
        1.6,
        False,
        (
            Subcategory("Camisetas", 30, 150),
            Subcategory("Calças", 80, 300),
            Subcategory("Calçados", 100, 600),
            Subcategory("Vestidos", 80, 400),
            Subcategory("Jaquetas", 150, 700),
        ),
    ),
    Department(
        "Casa e Decoração",
        1.0,
        False,
        (
            Subcategory("Cama, Mesa e Banho", 40, 400),
            Subcategory("Utensílios de Cozinha", 20, 300),
            Subcategory("Iluminação", 30, 350),
            Subcategory("Organização", 15, 200),
        ),
    ),
    Department(
        "Beleza e Saúde",
        0.9,
        False,
        (
            Subcategory("Perfumes", 80, 700),
            Subcategory("Cuidados com a Pele", 20, 250),
            Subcategory("Cabelos", 15, 150),
        ),
    ),
    Department(
        "Esporte e Lazer",
        0.6,
        False,
        (
            Subcategory("Bicicletas", 800, 4000),
            Subcategory("Fitness", 30, 600),
            Subcategory("Camping", 50, 900),
        ),
    ),
    Department(
        "Brinquedos",
        0.6,
        False,
        (
            Subcategory("Bonecas", 40, 300),
            Subcategory("Jogos de Tabuleiro", 50, 350),
            Subcategory("Blocos de Montar", 60, 800),
        ),
    ),
    Department(
        "Mercado",
        1.4,
        True,
        (
            Subcategory("Bebidas", 5, 120),
            Subcategory("Mercearia", 3, 60),
            Subcategory("Limpeza", 5, 80),
        ),
    ),
)

# Delivery time (days) from the São Paulo distribution centre, per customer region.
TRANSIT_DAYS: Final = {
    "Sudeste": (1, 4),
    "Sul": (2, 5),
    "Centro-Oeste": (3, 7),
    "Nordeste": (4, 9),
    "Norte": (6, 12),
}
FREIGHT_RANGE: Final = {
    "Sudeste": (12.0, 25.0),
    "Sul": (18.0, 35.0),
    "Centro-Oeste": (22.0, 45.0),
    "Nordeste": (25.0, 55.0),
    "Norte": (30.0, 70.0),
}
FREE_SHIPPING_FROM: Final = 299
