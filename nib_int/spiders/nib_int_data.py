import os
import string
from datetime import datetime
import pandas as pd
import scrapy
from scrapy.cmdline import execute


class NibIntDataSpider(scrapy.Spider):
    name = "nib_int_data"  # Name of the spider

    def __init__(self):
        super().__init__()
        self.data_list = None  # Placeholder for processed data

    def start_requests(self):
        """
        Initial request to scrape the webpage.
        """

        cookies = {
            'CookieConsent': '{stamp:%27neJFiZ9kYi5aHyPMEsyBLTS9rQ0OiIOegbvNEdD718QxXbvz0TtNnA==%27%2Cnecessary:true%2Cpreferences:false%2Cstatistics:false%2Cmarketing:false%2Cmethod:%27explicit%27%2Cver:2%2Cutc:1732857203681%2Cregion:%27in%27}',
        }

        headers = {
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'accept-language': 'en-US,en;q=0.9',
            'cache-control': 'no-cache',
            # 'cookie': 'CookieConsent={stamp:%27neJFiZ9kYi5aHyPMEsyBLTS9rQ0OiIOegbvNEdD718QxXbvz0TtNnA==%27%2Cnecessary:true%2Cpreferences:false%2Cstatistics:false%2Cmarketing:false%2Cmethod:%27explicit%27%2Cver:2%2Cutc:1732857203681%2Cregion:%27in%27}',
            'pragma': 'no-cache',
            'priority': 'u=0, i',
            'sec-ch-ua': '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
            'sec-fetch-dest': 'document',
            'sec-fetch-mode': 'navigate',
            'sec-fetch-site': 'same-origin',
            'sec-fetch-user': '?1',
            'upgrade-insecure-requests': '1',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
        }

        yield scrapy.Request(
            url="https://www.nib.int/who-we-are/compliance/debarred-entities",
            headers=headers,
            cookies=cookies,
            callback=self.parse
        )

    def parse(self, response, **kwargs):
        """
        Parse the response and process the data.
        """
        # Use pandas to extract tables from the HTML response
        dfs = pd.read_html(response.text, header=0)
        df = dfs[0]
        # Format column names to be consistent
        df.columns = self.format_columns(df)

        # Add an 'id' column with the index of each row
        df.insert(0, 'id', range(1, len(df) + 1))

        # Add a 'url' column with the current response URL
        df.insert(1, 'url', response.url)

        # Process data: convert dates, remove punctuation
        df = self.convert_to_date_format(df)
        df = self.remove_punctuation(df)

        # Ensure output directory exists
        os.makedirs('../output', exist_ok=True)

        # Save the processed data to an Excel file
        df.to_excel(f"../output/nib_int_{datetime.today().strftime('%Y%m%d')}.xlsx", index=False)

    def remove_punctuation(self, df):
        """
        Remove punctuation from the 'name_of_entity_or_individual' column and title-case the text.
        """
        if 'name_of_entity_or_individual' in df.columns:
            df['name_of_entity_or_individual'] = df['name_of_entity_or_individual'].apply(
                lambda x: ' '.join(
                    ''.join(
                        char if char not in string.punctuation else ' '
                        for char in str(x)
                    ).split()
                ) if pd.notnull(x) else x
            )
        return df

    def format_columns(self, df):
        """
        Standardize column names: replace spaces and slashes with underscores,
        strip whitespace, and convert to lowercase.
        """
        return (
            df.columns
            .str.replace(' ', '_', regex=False)
            .str.replace('/', '_', regex=False)
            .str.strip()
            .str.lower()
        )

    def convert_to_date_format(self, df):
        """
        Convert 'valid_from' and 'valid_until' columns to standardized date format (YYYY-MM-DD).
        """
        date_columns = ['valid_from', 'valid_until']
        for col in date_columns:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce').dt.strftime('%Y-%m-%d')
        return df


if __name__ == '__main__':
    # Execute the spider directly
    execute(f'scrapy crawl {NibIntDataSpider.name}'.split())
