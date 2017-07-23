using System;
using System.IO;

namespace Eventful
{
	public class DecimalReader : IDisposable
	{
		private const int bufferSize = 65536;
		private readonly Stream stream;
		private readonly byte[] byteBuffer;
		private readonly int[] bigIntBuffer;
		private readonly int[] smallIntBuffer;
		private readonly decimal[] decimalBuffer;
		private int bufferOffset = 0;

		public DecimalReader(Stream stream)
		{
			this.stream = stream;
			byteBuffer = new byte[bufferSize];
			bigIntBuffer = new int[bufferSize/4];
			smallIntBuffer = new int[4];
			decimalBuffer = new decimal[bufferSize/16];

			ReadDecimals();
		}

		private void ReadDecimals()
		{
			var numBytesRead = stream.Read(byteBuffer, 0, bufferSize);

			Buffer.BlockCopy(byteBuffer, 0, bigIntBuffer, 0, numBytesRead);

			for (var i = 0; i < bigIntBuffer.Length; i+=4)
			{
				smallIntBuffer[0] = bigIntBuffer[i + 0];
				smallIntBuffer[1] = bigIntBuffer[i + 1];
				smallIntBuffer[2] = bigIntBuffer[i + 2];
				smallIntBuffer[3] = bigIntBuffer[i + 3];

				decimalBuffer[i/4] = new Decimal(smallIntBuffer);
			}
		}

		public decimal ReadDecimal()
		{
			if (bufferOffset == decimalBuffer.Length)
			{
				ReadDecimals();
				bufferOffset = 0;
			}

			var d = decimalBuffer[bufferOffset];
			bufferOffset++;
			return d;
		}
		
		public void Dispose()
		{
			stream.Close();
		}
	}
}