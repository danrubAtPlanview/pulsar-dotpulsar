/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

namespace DotPulsar.Tests.Internal.Crypto;

using System.Numerics;
using System.Security.Cryptography;

// A slow and simple (not constant time) implementation of the arithmetic of a short Weierstrass curve y^2 = x^3 + ax + b over a prime field.
// It is used to check the shared secret of the platform ECDH against the definition (the x-coordinate of d * Q).
public sealed class ReferenceEc
{
    private readonly BigInteger _p;
    private readonly BigInteger _a;

    public ReferenceEc(ECCurve explicitCurve)
    {
        _p = ToBigInteger(explicitCurve.Prime!);
        _a = ToBigInteger(explicitCurve.A!);
        FieldLength = explicitCurve.Prime!.Length;
        Order = ToBigInteger(explicitCurve.Order!);
        Generator = new Point(ToBigInteger(explicitCurve.G.X!), ToBigInteger(explicitCurve.G.Y!));
    }

    public sealed record Point(BigInteger X, BigInteger Y);

    public int FieldLength { get; }
    public BigInteger Order { get; }
    public Point Generator { get; }

    public static BigInteger ToBigInteger(byte[] value)
        => new(value, isUnsigned: true, isBigEndian: true);

    public byte[] ToBytes(BigInteger value)
    {
        var bytes = value.ToByteArray(isUnsigned: true, isBigEndian: true);
        var padded = new byte[FieldLength];
        Buffer.BlockCopy(bytes, 0, padded, FieldLength - bytes.Length, bytes.Length);
        return padded;
    }

    public Point Multiply(BigInteger scalar, Point point)
    {
        Point? result = null;
        Point? addend = point;

        while (scalar > 0)
        {
            if (!scalar.IsEven)
                result = Add(result, addend);

            addend = Add(addend, addend);
            scalar >>= 1;
        }

        return result ?? throw new InvalidOperationException("The result is the point at infinity");
    }

    private Point? Add(Point? left, Point? right)
    {
        if (left is null)
            return right;

        if (right is null)
            return left;

        BigInteger lambda;

        if (left.X == right.X)
        {
            if (Mod(left.Y + right.Y) == 0)
                return null;

            lambda = Mod(((3 * left.X * left.X) + _a) * Invert(2 * left.Y));
        }
        else
            lambda = Mod((right.Y - left.Y) * Invert(right.X - left.X));

        var x = Mod((lambda * lambda) - left.X - right.X);
        var y = Mod((lambda * (left.X - x)) - left.Y);
        return new Point(x, y);
    }

    private BigInteger Mod(BigInteger value)
    {
        var result = value % _p;
        return result < 0 ? result + _p : result;
    }

    private BigInteger Invert(BigInteger value)
        => BigInteger.ModPow(Mod(value), _p - 2, _p);
}
