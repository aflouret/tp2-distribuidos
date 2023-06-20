
def retry(n : int):

    def __retry__(func):

        def wrapper(*args, **kwargs):

            for _ in range(n):
                res = func(*args, **kwargs)
                if res:
                    return res
        return wrapper

    return __retry__