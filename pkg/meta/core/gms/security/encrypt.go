/*
Copyright 2021 Alibaba Group Holding Limited.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package security

import (
	"crypto/aes"
	"crypto/cipher"
	"encoding/base64"
)

type AESCipher interface {
	Encrypt(data []byte) []byte
	Decrypt(enc []byte) ([]byte, error)
}

type aesCipher struct {
	block   cipher.Block
	padding PaddingScheme
}

func (c *aesCipher) Encrypt(data []byte) []byte {
	data = c.padding.Pad(data)

	enc := make([]byte, len(data))
	blockStart := 0
	for blockStart < len(data) {
		blockEnd := blockStart + c.block.BlockSize()
		c.block.Encrypt(enc[blockStart:blockEnd], data[blockStart:blockEnd])
		blockStart = blockEnd
	}

	return enc
}

func (c *aesCipher) Decrypt(enc []byte) ([]byte, error) {
	if len(enc)%c.block.BlockSize() != 0 {
		return nil, ErrNotAlignedToBlock
	}

	data := make([]byte, len(enc))
	blockStart := 0
	for blockStart < len(data) {
		blockEnd := blockStart + c.block.BlockSize()
		c.block.Decrypt(data[blockStart:blockEnd], enc[blockStart:blockEnd])
		blockStart = blockEnd
	}

	return c.padding.Strip(data)
}

func NewAESCipherWithPKCS7Padding(key []byte) (AESCipher, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}

	return &aesCipher{
		block:   block,
		padding: NewPkcs7(block.BlockSize()),
	}, nil
}

type PasswordCipher interface {
	Key() string
	Encrypt(passwd string) string
	Decrypt(enc string) (string, error)
}

type passwordCipher struct {
	key       string
	aesCipher AESCipher
}

func (c *passwordCipher) Key() string {
	return c.key
}

func (c *passwordCipher) Encrypt(passwd string) string {
	enc := c.aesCipher.Encrypt([]byte(passwd))
	return base64.StdEncoding.EncodeToString(enc)
}

func (c *passwordCipher) Decrypt(enc string) (string, error) {
	b, err := base64.StdEncoding.DecodeString(enc)
	if err != nil {
		return "", err
	}

	b, err = c.aesCipher.Decrypt(b)
	if err != nil {
		return "", err
	}

	return string(b), err
}

func NewPasswordCipher(key string) (PasswordCipher, error) {
	aesCipher, err := NewAESCipherWithPKCS7Padding([]byte(key))
	if err != nil {
		return nil, err
	}
	return &passwordCipher{
		key:       key,
		aesCipher: aesCipher,
	}, nil
}

func MustNewPasswordCipher(key string) PasswordCipher {
	c, err := NewPasswordCipher(key)
	if err != nil {
		panic(err)
	}
	return c
}
