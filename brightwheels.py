import requests as r
from bs4 import BeautifulSoup
import pandas as pd
import ssl
from urllib.request import urlopen

ssl._create_default_https_context = ssl._create_unverified_context

class brightWheels:
    def __init__(self):
        self.src_file = 'data/x_ca_omcc_providers.csv'
        self.src_omcc_url = 'https://naccrrapps.naccrra.org/navy/directory/programs.php?program=omcc&state=CA&pagenum={}'
        self.internal_api = 'https://bw-interviews.herokuapp.com/data/providers'

    def extract_html_data(self):
        """ I had to write custom code for extracting the data from OMCC website due to the structure of the website
        Normally, pandas is able to read the HTML content using read_html(). However, in this case it did not work.
        So I wrote custom code to fetch the data from all pages and saved it in csv which I used later. This function
        was not done within the 2 hours window as suggested."""

        final_data = []
        for i in range(1, 48):
            response = urlopen(self.src_omcc_url.format(i))
            html_doc = response.read()
            table_content = BeautifulSoup(html_doc, 'html.parser').table.contents
            table_col = table_content[1].text.split('\n')[2:]
            content = table_content[3:]
            for i in range(0, len(content), 8):
                temp_dict={}
                temp_dict[table_col[0]] = content[i]
                temp_dict[table_col[1]] = content[i+1]
                temp_dict[table_col[2]] = content[i+2]
                temp_dict[table_col[3]] = content[i+3]
                temp_dict[table_col[4]] = content[i+4]
                temp_dict[table_col[5]] = content[i+5]
                temp_dict[table_col[6]] = content[i+6]
                temp_dict[table_col[7]] = content[i+7]
                final_data.append(temp_dict)

        df = pd.DataFrame(final_data)
        df.to_csv("final_data.csv")

    def extract_internal_api_data(self):
        internal_api_resp = r.get(self.internal_api).json()
        providers_lst = []
        for providers in internal_api_resp["providers"]:
            providers_lst.append([providers['provider_name'], providers['phone'], providers['email'],providers['owner_name']])

        provider_df = pd.DataFrame(providers_lst)
        return(provider_df)
        #provider_df.to_csv('internal_api.csv')

    def transform_source_data(self):
        """Assumption: Join based on Phone number"""
        api_df = self.extract_internal_api_data()
        api_df.columns = ["provider_api", "phone_api", "email_api", "owner_api"]
        print('api size: {}'.format(api_df.shape))

        csv_df = pd.read_csv(self.src_file)
        csv_df.columns = ["provider_csv", "type_csv", "address_csv", "city_csv", "state_csv", "zipcode_csv", "phone_csv"]

        csv_df['phoneNumber_csv'] = csv_df['phone_csv'].astype(str).apply(lambda x: '('+x[:3]+') '+x[3:6]+'-'+x[6:10])

        api_csv = pd.merge(left=csv_df, right=api_df, how='outer', left_on='phoneNumber_csv',
                              right_on='phone_api')
        api_csv['provider_api_csv'] = api_csv['provider_csv'].combine_first(api_csv['provider_api'])
        api_csv['phone_api_csv'] = api_csv['phoneNumber_csv'].combine_first(api_csv['phone_api'])
        api_csv = api_csv.drop(['provider_csv', 'provider_api', 'phoneNumber_csv', 'phone_csv', 'phone_api'], 1)

        api_csv.to_csv('data/api_csv.csv')

        html_df = pd.read_csv('data/final_data.csv')
        print('html size: {}'.format(html_df.shape))

        merged_df = pd.merge(left=html_df, right=api_csv, how='outer', left_on='Phone',
                                right_on='phone_api_csv')

        merged_df['Provider'] = merged_df['Provider'].combine_first(api_csv['provider_api_csv'])
        merged_df['Type'] = merged_df['Type'].combine_first(api_csv['type_csv'])
        merged_df['Address'] = merged_df['Address'].combine_first(api_csv['address_csv'])
        merged_df['City'] = merged_df['City'].combine_first(api_csv['city_csv'])
        merged_df['State'] = merged_df['State'].combine_first(api_csv['state_csv'])
        merged_df['Zip'] = merged_df['Zip'].combine_first(api_csv['zipcode_csv'])
        merged_df['Phone'] = merged_df['Phone'].combine_first(api_csv['phone_api_csv'])
        merged_df['Email'] = merged_df['Email '].combine_first(api_csv['email_api'])
        merged_df['owner'] = api_csv['owner_api']

        merged_df = merged_df.drop(['Email ', 'provider_api_csv', 'type_csv', 'address_csv', 'city_csv', 'state_csv',
                                      'zipcode_csv', 'phone_api_csv', 'email_api', 'owner_api'], 1)

        merged_df.to_csv('data/merged.csv')
        print(merged_df.shape)

def main():
    bw = brightWheels()
    #bw.extract_internal_api_data()
    bw.transform_source_data()


# Standard boiler plate
if __name__ == "__main__":
    main()
