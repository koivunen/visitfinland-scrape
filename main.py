from dotenv import load_dotenv
import os
import pathlib
import asyncio

from gql import Client, gql
from gql.transport.aiohttp import AIOHTTPTransport
from gql.transport.exceptions import TransportServerError

load_dotenv()
headers_graphql_apikey: dict[str, str] = {
    'ocp-apim-subscription-key': os.getenv('DATAHUB_API_KEY',''),
}
if headers_graphql_apikey['ocp-apim-subscription-key'] == '':
    raise ValueError("DATAHUB_API_KEY is not set")

DATAHUB_URL = 'https://api.businessfinland.fi/traveldatahub'

async def main():
    transport = AIOHTTPTransport(url=DATAHUB_URL, headers=headers_graphql_apikey)
    client = Client(transport=transport,fetch_schema_from_transport=False)

    QUERY_ALL = pathlib.Path("all_products_all_data.graphql.txt").read_text()
    async with client as session:

        products = [] # 2026 there were around 13k products
        limit = 200
        for i in range(0, 20000, limit):
            await asyncio.sleep(1.1) # 60 calls per minute max: https://developer.businessfinland.fi/
            query = gql(QUERY_ALL)
            query.variable_values = {"limit": limit, "offset": i}
            try:
                result = await session.execute(query)
            except TimeoutError as e:
                print(f"TimeoutError at offset {i}: {e}, aborting...")
                raise
            except TransportServerError as e:
                print(f"TransportServerError at offset {i}: {e}, aborting...")
                print(e.code, e.args)
                raise
            except Exception as e:
                print(f"Error fetching data at offset {i}: {e}")
                raise

            if result['product'] == []:
                break
            products.extend(result['product'])
            print(f"Fetched {len(products)} products so far...")
            
        #print(result)
        with pathlib.Path("all_products_all_data.json").open("w", encoding="utf-8") as f:
            import json
            json.dump(products, f, ensure_ascii=False, indent='\t')


asyncio.run(main())

#TODO
"""Exception has occurred: TransportServerError
429, message='Too Many Requests', url='https://api.businessfinland.fi/traveldatahub'
aiohttp.client_exceptions.ClientResponseError: 429, message='Too Many Requests', url='https://api.businessfinland.fi/traveldatahub'"""