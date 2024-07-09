from logging import Logger, getLogger


def get_logger() -> Logger:
    return getLogger(f'sl3aio.{__name__}')
