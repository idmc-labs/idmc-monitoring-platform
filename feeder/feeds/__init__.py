from lxml import etree
import requests
from typing import List


class XMLToJSONParser():
    def __init__(self, url):
        self.url = url

    def fetch_content(self):
        response = requests.get(self.url)
        return response.content

    def get_json_data(self, xml_element, attributes_required: List, keep_original_xml=list()) -> dict:
        """
        returns a json from xml element
        """
        try:
            tag = etree.QName(xml_element).localname
        except ValueError:
            return {}
        if tag in keep_original_xml:
            return {tag: etree.tostring(xml_element).decode()}
        if len(xml_element) == 0:
            # there are no children
            d = {tag: xml_element.text}
            
            if tag in attributes_required:
                # append the attributes as well
                for k, v in dict(xml_element.attrib).items():
                    d[k] = v
        else:
            d = {
                tag: [
                    self.get_json_data(child, attributes_required, keep_original_xml)
                    for child in xml_element
                ]
            }
            if tag in attributes_required:
                # append the attributes as well
                for k, v in dict(xml_element.attrib).items():
                    d[k] = v
        return d

    def get_features(self, attributes_required: List, keep_original_xml: List):
        """
        returns the json {tag: text} attributes obtained from the gdacs rss feed
        """
        content = self.fetch_content()
        root = etree.fromstring(content)
        return self.get_json_data(root, attributes_required, keep_original_xml)
    
    def __call__(self, attributes_required=list(), keep_original_xml=list()):
        return self.get_features(attributes_required, keep_original_xml)
