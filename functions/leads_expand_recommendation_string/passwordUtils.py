import boto3

client = boto3.client('ssm')
next_token = None
velocify_passwords = None


def generate_velocify_passwords(env, refresh=False):
    global velocify_passwords

    if velocify_passwords and not refresh:
        print("::::: Velocify Passwords Already Generated")
        print velocify_passwords
        return velocify_passwords

    passwords_dict = {}
    parameter_list = []

    try:
        parameter_list = get_passwords(env)
    except Exception as e:
        print(":::: ERROR: Couldn't Fetch Velocify Passwords")
        print(e)

    print("::::: Generating Velocify Passwords")

    for p in parameter_list:
        key = p['Name'].replace("/{}/velocifyPasswords/".format(env), '')
        passwords_dict[key] = p['Value']

    velocify_passwords = passwords_dict

    print("::::: Succesfully Generated Velocify Passwords")

    return velocify_passwords


def get_passwords(env):
    global next_token
    print("::::: Fetching Velocify Passwords")

    if next_token == None:
        parameters = client.get_parameters_by_path(
            Path="/{}/velocifyPasswords".format(env),
            WithDecryption=True,
            MaxResults=1
        )
    else:
        print "::::: Fetching next set"
        parameters = client.get_parameters_by_path(
            Path="/{}/velocifyPasswords".format(env),
            WithDecryption=True,
            NextToken=next_token,
            MaxResults=1
        )

    if 'NextToken' in parameters:
        next_token = parameters['NextToken']
        next_passwords = get_passwords()
        return parameters['Parameters'] + next_passwords

    return parameters['Parameters']
