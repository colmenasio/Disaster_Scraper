class Article:
    def __init__(self, title_arg: str, link_arg: str, date_arg: str):
        self.title = title_arg
        self.link = link_arg
        self.date = date_arg

    def __repr__(self):
        return f"<__main__.Article object: {self.title}, {self.link}, {self.date}>"
