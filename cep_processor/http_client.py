from httpx import AsyncClient, Limits, Timeout


CEP_API_URL = 'https://viacep.com.br/ws/{CEP}/json/'


class CepHttpClient():
    def __init__(self, max_http_connections: int = 50):
        self._client = AsyncClient(
            limits=Limits(max_connections=max_http_connections),
            timeout=Timeout(timeout=30, connect=None, pool=None)
        )

    async def request_cep_api(self, cep: str):
        url = CEP_API_URL.format(CEP=cep)
        try:
            response = await self._client.get(url)
            if response.is_success:
                data = response.json()
                return {
                    'result': data,
                    'success': True
                }
            else:
                print('Ops.. an error occurred')
                print(response.content)
                return {
                    'result': response.text,
                    'success': False
                }
        except Exception as ex:
            print('Ops... Exception')
            print(f'Exception: {type(ex)} Error: {str(ex)}')
            return {
                'result': f'Exception: {type(ex)} Error: {str(ex)}',
                'success': False
            }


    async def close(self):
        await self._client.aclose()
