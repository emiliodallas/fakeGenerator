from faker import Faker
import pandas as pd
import random

fake = Faker()

def fakeGenerator(value):
    """
    Introduce errors to the given value based on the error rate.

    :param value: The value to potentially introduce errors to
    :param error_rate: Probability of introducing errors (0.0 to 1.0)
    :return: The original value with potential errors
    """
    error_rate = 0.1

    if random.random() < error_rate:
        # Introduce an error, e.g., truncate the value or replace with a random character
        value = value[:random.randint(0, len(value)-1)] if value else value
    return value

def generate_fake_user_data(num_users = 1000):
    """
    Generate generic fake data using Fake Library.

    :param num_users: The number of users to generate (default: 1000)
    :return: A Pandas DataFrame containing the generated user data
    """
    fake = Faker()
    users = []

    for _ in range(num_users):
        user = {
            'user_id': fake.uuid4(),
            'username': fake.user_name(),
            'email': fakeGenerator(fake.email()),
            'phone': fakeGenerator(fake.phone_number()),
            'registration_date': fake.date_time_this_decade(),
            'last_login_date': fake.date_time_this_year(),
        }
        users.append(user)

    # Convert the data to a Pandas DataFrame
    df = pd.DataFrame(users)
    return df
 
def generate_fake_activities(num_activities, num_users):
    """
    Generates fake activities for the users on the fake site

    :param num_activities: The number of activities to generate(default: 1000)
    :param num_users: The number of users to generate (default: 1000)
    :return: A Pandas DataFrame containing the generated user data
    """
    activity_types = ["login", "purchase", "comment", "favorite", "like", "dislike", "cart", "wishlist"]
    activities = []

    for _ in range(num_activities):
        activity = {
            'activity_id': fake.uuid4(),
            'user_id': fake.random_element([f"user-{i}" for i in range(1, num_users+1)]),
            'activity_type': fake.random_element(activity_types),
            'activity_timestamp': fake.date_time_this_year(),
            'activity_details': fake.sentence(),
        }
        activities.append(activity)
    
    # Convert the data to a Pandas DataFrame
    df = pd.DataFrame(activities)
    return df

def generate_fake_activity_types(num_types):
    """
    Generates fake activity types for the users on the fake site

    :param num_types: The number of types to generate(default: 10)
    :return: A Pandas DataFrame containing the generated user data
    """
    parent = ["female", "male"]
    child = ["pants", "flip flop", "t-shirt", "skirt", "shirt", "pajamas", "shoes", "watch", "belt", "socks", "underwear", "bra", "jacket"]
    activity_type_data = []
    for i in range(num_types):
        activity_type = {
            'type_id': i + 1,
            'parent_type_id': fake.random_element(parent),
            'type_name': fake.random_element(child)
        }
        activity_type_data.append(activity_type)
    
    df = pd.DataFrame(activity_type_data)
    return df

def save_data_to_csv(df, filename):
    """
    Save the generated user data to a CSV file.

    :param df: The Pandas DataFrame containing user data
    :param filename: The name of the CSV file
    """
    df.to_csv(filename, index=False)

if __name__ == "__main__":
    # Generate user data and save it to a CSV file
    user_data = generate_fake_user_data(num_users=1000)
    activity = generate_fake_activities(num_activities=1000, num_users=1000)
    types = generate_fake_activity_types(num_types=100)

    save_data_to_csv(user_data, filename="user_data.csv")
    save_data_to_csv(activity, filename="activity.csv")
    save_data_to_csv(types, filename="types.csv")