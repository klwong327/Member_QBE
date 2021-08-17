
from datetime import datetime
def calculate_age(born, date=datetime.today()):
 
  #today = datetime.today()
  try: 
      birthday = born.replace(year = date.year)

  # raised when birth date is February 29
  # and the current year is not a leap year
  except ValueError: 
      birthday = born.replace(year = date.year,
                month = born.month + 1, day = 1)

  if birthday > date:
      return date.year - born.year - 1
  else:
      return date.year - born.year 