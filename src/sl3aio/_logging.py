from logging import Logger, getLogger


def get_logger(name: str) -> Logger:
    return getLogger(f'sl3aio.{name}')
