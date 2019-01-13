#!/usr/bin/env python3

import requests
import memcache
import json
from hashlib import md5

USER_ID = '4b2ea5dd72ed1cad7a82b739c48ce98c'
USER_SECRET = '2d3e2a2b857ebc72219047dc4f4aa35c'


class Token:
    auth_url = 'https://api.sendpulse.com/oauth/access_token'
    mc = memcache.Client(['127.0.0.1:11211'], debug=0)

    def __init__(self, user_id, secret):
        self.user_id = user_id
        self.user_secret = secret
        self.token_name = self.create_token_name()
        self.token = self.set_token()

    def create_token_name(self):
        m = md5()
        m.update('{}::{}'.format(self.user_id, self.user_secret).encode('utf-8'))
        name = m.hexdigest()
        return name

    def set_token(self):
        if Token.mc.get(self.token_name):
            return Token.mc.get(self.token_name)
        else:
            auth_data = dict(
                grant_type='client_credentials',
                client_id=self.user_id,
                client_secret=self.user_secret
            )
            print(Token.auth_url)
            print(json.dumps(auth_data))
            r = requests.post(Token.auth_url, json=auth_data)
            t = '{} {}'.format(r.json()['token_type'], r.json()['access_token'])
            ttl = r.json()['expires_in']
            Token.mc.set(self.token_name, t, ttl-20)
            return t

    def get_token(self):
        return self.token

    def get_token_name(self):
        return self.token_name


if __name__ == '__main__':
    t = Token(USER_ID, USER_SECRET).get_token()
    t_name = Token(USER_ID, USER_SECRET).get_token_name()
    print('{} ==> "{}"'.format(t_name, t))