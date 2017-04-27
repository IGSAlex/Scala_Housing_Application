
import java.util.Base64;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.KeyGenerator;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;
import javax.crypto.spec.IvParameterSpec;

import sun.misc.BASE64Encoder;

object Main {
    def main(args: Array[String]) {
      
     /**
			 * Step 1. Generate an AES key using KeyGenerator Initialize the
			 * keysize to 128 bits (16 bytes)
			 * 
			 */
      
      var keyGen:KeyGenerator  = KeyGenerator.getInstance("AES");
      keyGen.init(128);
			var secretKey:SecretKey= keyGen.generateKey();
			
     System.out.println("SecretKey using is "	+ secretKey.toString());
			
			/**
			 * Step 2. Generate an Initialization Vector (IV) 
			 * 		a. Use SecureRandom to generate random bits
			 * 		   The size of the IV matches the blocksize of the cipher (128 bits for AES)
			 * 		b. Construct the appropriate IvParameterSpec object for the data to pass to Cipher's init() method
			 */

			var AES_KEYLENGTH:Int = 128;	// change this as desired for the security level you want
			
			
			//var iv = Array[Byte](16);	// Save the IV bytes or send it in plaintext with the encrypted data so you can decrypt the data later
			var prng = new SecureRandom();
			//prng.nextBytes(iv);
			
			var iv = prng.generateSeed(16)

			System.out.println("IV using is "	+ Base64.getEncoder().encodeToString(iv));
			
			/**
			 * Step 3. Create a Cipher by specifying the following parameters
			 * 		a. Algorithm name - here it is AES 
			 * 		b. Mode - here it is CBC mode 
			 * 		c. Padding - e.g. PKCS7 or PKCS5
			 */

			var aesCipherForEncryption :Cipher = Cipher.getInstance("AES/CBC/PKCS5PADDING"); // Must specify the mode explicitly as most JCE providers default to ECB mode!!

			/**
			 * Step 4. Initialize the Cipher for Encryption
			 */

			aesCipherForEncryption.init(Cipher.ENCRYPT_MODE, secretKey, new IvParameterSpec(iv));

			/**
			 * Step 5. Encrypt the Data 
			 * 		a. Declare / Initialize the Data. Here the data is of type String 
			 * 		b. Convert the Input Text to Bytes 
			 * 		c. Encrypt the bytes using doFinal method
			 */
			var strDataToEncrypt:String = "Hello World of Encryption using AES ";
		  var byteDataToEncrypt:Array[Byte]  = strDataToEncrypt.getBytes();
			var byteCipherText:Array[Byte] = aesCipherForEncryption.doFinal(byteDataToEncrypt);
			// b64 is done differently on Android
			var strCipherText: String =Base64.getEncoder().encodeToString(byteCipherText);
			System.out.println("Cipher Text generated using AES is "
					+ strCipherText);

			/**
			 * Step 6. Decrypt the Data 
			 * 		a. Initialize a new instance of Cipher for Decryption (normally don't reuse the same object)
			 * 		   Be sure to obtain the same IV bytes for CBC mode.
			 * 		b. Decrypt the cipher bytes using doFinal method
			 */

			var aesCipherForDecryption:Cipher = Cipher.getInstance("AES/CBC/PKCS5PADDING"); // Must specify the mode explicitly as most JCE providers default to ECB mode!!				

			aesCipherForDecryption.init(Cipher.DECRYPT_MODE, secretKey,new IvParameterSpec(iv));
			
			//var byteDecryptedText:Array[Byte] = aesCipherForDecryption.doFinal(byteCipherText);

			var byteDecryptedText:Array[Byte] = aesCipherForDecryption.doFinal( Base64.getDecoder().decode(strCipherText) );
			var strDecryptedText:String = new String(byteDecryptedText);
			System.out.println(" Decrypted Text message is " + strDecryptedText);
			
    }
}

