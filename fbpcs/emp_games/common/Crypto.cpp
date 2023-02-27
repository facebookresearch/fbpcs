/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "fbpcs/emp_games/common/Crypto.h"

#include <openssl/conf.h>
#include <openssl/err.h>
#include <openssl/evp.h>
#include <stdexcept>

namespace private_measurement::crypto {

HybridCipher::CipherMessage HybridCipher::encrypt(
    const std::vector<unsigned char>& plaintext,
    folly::ssl::EvpPkeySharedPtr pubKey) {
  // Obtain parameters for encryption
  const size_t kBlockSize = EVP_CIPHER_block_size(kSymmetricCipher);
  const size_t kIvLength = EVP_CIPHER_iv_length(kSymmetricCipher);
  const size_t publickKeyLength = EVP_PKEY_size(pubKey.get());

  // Create space for storing the symmetric key, iv, and output ciphertext
  CipherMessage msg(
      plaintext.size() + kBlockSize, // ciphertext length
      kIvLength, // iv length
      publickKeyLength, // session key length
      0 // signature length
  );
  int32_t sessionKeyLen;

  // Initialize context
  folly::ssl::EvpCipherCtxUniquePtr ctx(EVP_CIPHER_CTX_new());
  int32_t ciphertextLen;
  int32_t len;

  // Initialise the envelope seal operation. This operation generates
  // a session key for the provided cipher, and then encrypts that key a number
  // of times (one for each public key provided in the pub_key array).
  // Here the array size is just one. This operation also
  // generates an IV and places it in iv.
  unsigned char* sessionKeyWrap[1] = {msg.sessionKey.data()};
  EVP_PKEY* keyWrap[1] = {pubKey.get()};
  checkSuccessOrThrow(
      EVP_SealInit(
          ctx.get(), // EVP_CIPHER_CTX * EVP_context
          kSymmetricCipher, // const EVP_CIPHER * EVP_cipher
          sessionKeyWrap, // unsigned char ** session_keys_loc
          &sessionKeyLen, // int * session_key_length_loc
          msg.iv.data(), // unsigned char * iv_loc
          keyWrap, // EVP_PKEY ** pubkeys_loc
          1), // int num_pubkeys
      "EVP_SealInit");
  msg.sessionKey.resize(sessionKeyLen);

  // Provide the message to be encrypted, and obtain the encrypted output.
  checkSuccessOrThrow(
      EVP_SealUpdate(
          ctx.get(), // EVP_CIPHER_CTX * EVP_context
          msg.ciphertext.data(), // unsigned char * output_location
          &len, // int * output_length
          plaintext.data(), // unsigned char * input_location
          plaintext.size()), // int input_length
      "EVP_SealUpdate failed");
  ciphertextLen = len;

  // Finalise the encryption.
  checkSuccessOrThrow(
      EVP_SealFinal(ctx.get(), msg.ciphertext.data() + len, &len),
      "EVP_SealFinal failed");
  ciphertextLen += len;
  msg.ciphertext.resize(ciphertextLen);

  return msg;
}

std::vector<unsigned char> HybridCipher::decrypt(
    const HybridCipher::CipherMessage& msg,
    folly::ssl::EvpPkeySharedPtr privKey) {
  folly::ssl::EvpCipherCtxUniquePtr ctx(EVP_CIPHER_CTX_new());
  int len;
  int plaintextLen;

  std::vector<unsigned char> plaintext(msg.ciphertext.size());
  // Initialise the decryption operation. The asymmetric private key is
  // provided as privKey, whilst the encrypted session key is held in
  // sessionKey
  checkSuccessOrThrow(
      EVP_OpenInit(
          ctx.get(), // EVP_CIPHER_CTX * ctx
          kSymmetricCipher, // const EVP_CIPHER * EVP_cipher
          msg.sessionKey.data(), // unsigned char * session_key
          msg.sessionKey.size(), // int session_key_length
          msg.iv.data(), // unsigned char * iv
          privKey.get()), // EVP_PKEY * private_key
      "EVP_OpenInit failed.");

  // Provide the message to be decrypted, and obtain the plaintext output.
  checkSuccessOrThrow(
      EVP_OpenUpdate(
          ctx.get(), // EVP_CIPHER_CTX * ctx
          plaintext.data(), // unsigned char* output
          &len, // int * output length
          msg.ciphertext.data(), // unsigned char * input
          msg.ciphertext.size()), // int input length
      "EVP_OpenUpdate failed.");
  plaintextLen = len;

  // Finalise the decryption.
  checkSuccessOrThrow(
      EVP_OpenFinal(ctx.get(), plaintext.data() + len, &len),
      "EVP_OpenFinal failed");
  plaintextLen += len;
  plaintext.resize(plaintextLen);

  return plaintext;
}

} // namespace private_measurement::crypto
