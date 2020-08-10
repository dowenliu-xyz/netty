/*
 * Copyright 2015 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package io.netty.handler.codec.protobuf;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.nano.CodedInputByteBufferNano;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.CorruptedFrameException;

import java.util.List;

/**
 * A decoder that splits the received {@link ByteBuf}s dynamically by the
 * value of the Google Protocol Buffers
 * <a href="https://developers.google.com/protocol-buffers/docs/encoding#varints">Base
 * 128 Varints</a> integer length field in the message. For example:
 * 按消息中 Google Protocal Buffers
 * <a href="https://developers.google.com/protocol-buffers/docs/encoding#varints">Base
 * 128 Varints</a> 类型长度字段切分 {@link ByteBuf} 的解码器。示例：
 * <pre>
 * BEFORE DECODE (302 bytes)       AFTER DECODE (300 bytes)
 * 解码前 (302 字节)                 解码后 (300 字节)
 * +--------+---------------+      +---------------+
 * | Length | Protobuf Data |----->| Protobuf Data |
 * | 0xAC02 |  (300 bytes)  |      |  (300 bytes)  |
 * +--------+---------------+      +---------------+
 * </pre>
 *
 * @see CodedInputStream
 * @see CodedInputByteBufferNano
 */
public class ProtobufVarint32FrameDecoder extends ByteToMessageDecoder {

    // TODO maxFrameLength + safe skip + fail-fast option
    //      (just like LengthFieldBasedFrameDecoder)

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out)
            throws Exception {
        in.markReaderIndex(); // 标记以备重置读指针
        int preIndex = in.readerIndex(); // 记录读之前读指针位置
        int length = readRawVarint32(in); // 读varint32值，消息体长度
        if (preIndex == in.readerIndex()) { // 读指针没有移动，没有读出 varint32值，等待下次调用
            return;
        }
        if (length < 0) { // 长度是负值，异常
            throw new CorruptedFrameException("negative length: " + length);
        }

        if (in.readableBytes() < length) { // 累积缓冲区中没有足够的数据，不能解码出一个 message，即半包状态
            // 重置读指针，等待下一次 channelRead 调用或 channelInactive 调用或 ChannelInputShutdownEvent 事件回调
            in.resetReaderIndex();
        } else {
            out.add(in.readRetainedSlice(length)); // 解码出一个 message
        }
    }

    /**
     * Reads variable length 32bit int from buffer
     * 从缓冲区中读取变长32位整型值。
     *
     * @return decoded int if buffers readerIndex has been forwarded else nonsense value
     * 如果缓冲区读指针前移了，返回解码出的整数值；否则返回无意义值
     */
    private static int readRawVarint32(ByteBuf buffer) {
        if (!buffer.isReadable()) { // 缓冲区中没有数据
            return 0;
        }
        buffer.markReaderIndex(); // 标记以备重置读指针
        byte tmp = buffer.readByte(); // 读第一个字节
        if (tmp >= 0) { // 没有 msb 标记，小于 128 的值，直接返回
            return tmp;
        } else { // else 没有必要，因为 if 已经 return
            // 有 msb 标记，需要读第二字节
            int result = tmp & 127; // 去除 msb 标记，作为低 7 - 0 位记录
            if (!buffer.isReadable()) { // 累积缓冲区中没有后续数据
                buffer.resetReaderIndex(); // 重置读指针，等待下次调用
                return 0;
            }
            // 读第二个字节
            if ((tmp = buffer.readByte()) >= 0) { // 没有 msb 标记，varint 结束。
                result |= tmp << 7; // 第二字节低 7 位作为结果的低 14 - 8 位。解码结束。
                // 可以提前 return ，减少嵌套缩进
            } else {
                // 第二字节有 msb 标记，需要读第三字节
                result |= (tmp & 127) << 7; // 第二字节低 7 位（去除 msb）作为结果的低 14 - 8 位。
                if (!buffer.isReadable()) { // 累积缓冲区中没有后续数据
                    buffer.resetReaderIndex(); // 重置读指针，等待下次调用
                    return 0;
                }
                // 读第三字节
                if ((tmp = buffer.readByte()) >= 0) { // 没有 msb 标记，varint 结束
                    result |= tmp << 14; // 第三字节低 7 位作为结果的低 21 - 15 位。解码结束
                    // 可以提前 return ，减少嵌套缩进
                } else {
                    // 第三字节有 msb 标记，需要读第四字节
                    result |= (tmp & 127) << 14; // 第三字节低 7 位（去除 msb）作为结果的低 21 - 15 位。
                    if (!buffer.isReadable()) { // 累积缓冲区中没有后续数据
                        buffer.resetReaderIndex(); // 重置读指针，等待下次调用
                        return 0;
                    }
                    // 读第四字节
                    if ((tmp = buffer.readByte()) >= 0) { // 没有 msb 标记，varint 结束
                        result |= tmp << 21; // 第四字节低 7 位作为结果的低 28 - 22 位。解码结束
                        // 可以提前 return ，减少嵌套缩进
                    } else {
                        // 第四字节有 msb 标记，需要读最后一个字节，第五字节
                        result |= (tmp & 127) << 21; // 第四字节低 7 位（去除 msb）作为结果的低 28 -22 位。
                        if (!buffer.isReadable()) { // 累积缓冲区中没有后续数据
                            buffer.resetReaderIndex(); // 重置读指针，等待下次调用
                            return 0;
                        }
                        result |= (tmp = buffer.readByte()) << 28; // 第五字节低 4 位作为结果的低 32 - 29 位。
                        // 可能最终结果高 1 位是 1，导致整体值为负数
                        if (tmp < 0) { // 第五字节是最后一个字节，不能有 msb 标记
                            throw new CorruptedFrameException("malformed varint.");
                        }
                    }
                }
            }
            return result;
        }
    }
}
