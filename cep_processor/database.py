from sqlalchemy import String
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import Mapped, mapped_column, registry

table_registry = registry()


@table_registry.mapped_as_dataclass
class Cep:
    __tablename__ = 'ceps'

    id: Mapped[int] = mapped_column(init=False, primary_key=True)
    cep: Mapped[str] = mapped_column(String(8), index=True)
    logradouro: Mapped[str | None] = mapped_column(String(150))
    complemento: Mapped[str | None] = mapped_column(String(150))
    unidade: Mapped[str | None] = mapped_column(String(150))
    bairro: Mapped[str | None] = mapped_column(String(150))
    localidade: Mapped[str | None] = mapped_column(String(150))
    uf: Mapped[str | None] = mapped_column(String(2))
    estado: Mapped[str | None] = mapped_column(String(150))
    regiao: Mapped[str | None] = mapped_column(String(150))
    ibge: Mapped[str | None] = mapped_column(String(20))
    gia: Mapped[str | None] = mapped_column(String(20))
    ddd: Mapped[str | None] = mapped_column(String(2))
    siafi: Mapped[str | None] = mapped_column(String(20))


class Database:
    def __init__(self):
        self._engine = create_async_engine(
            'postgresql+psycopg://postgres:db_passwd@localhost:5432/ceps',
            pool_size=20,
            max_overflow=5,
        )

    async def create_tables(self):
        async with self._engine.begin() as conn:
            await conn.run_sync(table_registry.metadata.create_all)

    async def save_cep(self, cep_data):
        async with AsyncSession(
            self._engine, expire_on_commit=False
        ) as session:
            try:
                cep = Cep(
                    cep=cep_data['cep'].replace('-', ''),
                    logradouro=cep_data['logradouro'],
                    complemento=cep_data['complemento'],
                    unidade=cep_data['unidade'],
                    bairro=cep_data['bairro'],
                    localidade=cep_data['localidade'],
                    uf=cep_data['uf'],
                    estado=cep_data['estado'],
                    regiao=cep_data['regiao'],
                    ibge=cep_data['ibge'],
                    gia=cep_data['gia'],
                    ddd=cep_data['ddd'],
                    siafi=cep_data['siafi'],
                )
                session.add(cep)
                await session.commit()
                await session.refresh(cep)
            except Exception as ex:
                print("Ops... Can't save registry into the database")
                print(f'Exception type {type(ex)} Error: {ex}')

    async def close(self):
        await self._engine.dispose()
