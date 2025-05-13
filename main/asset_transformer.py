import random
import string


def randomize_string(s, num_changes):
    """
    Randomly changes 'num_changes' characters in the string 's'.
    """
    if num_changes > len(s):
        raise ValueError("Number of changes exceeds string length.")
    
    s_list = list(s)
    indices = random.sample(range(len(s_list)), num_changes)
    for idx in indices:
        s_list[idx] = random.choice(string.ascii_letters + string.digits)
    return ''.join(s_list)

def get_modified_assets(asset_list, num_changes):
    """
    Selects two random elements from 'asset_list' and modifies each by changing
    'num_changes' characters.
    """
    if len(asset_list) < 2:
        raise ValueError("Asset list must contain at least two elements.")
    
    selected_assets = random.sample(asset_list, 2)
    modified_assets = [randomize_string(asset, num_changes) for asset in selected_assets]
    return modified_assets
