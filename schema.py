class Document:
    def __init__(self, page_content:str, metadata:dict):
        
        """
        Parameters:
            page_content (str): Text.
            metadata (dict): Metadata for the text.
        """

        self.page_content = page_content
        self.metadata = metadata