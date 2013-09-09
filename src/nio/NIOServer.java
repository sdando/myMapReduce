package nio;

import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Iterator;


public class NIOServer {

	private static final int TIMEOUT  = 300;
	private static final int PORT     = 12112;
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		try {
			Selector selector=Selector.open();
			ServerSocketChannel listenChannel=ServerSocketChannel.open();
			listenChannel.configureBlocking(false);
			listenChannel.socket().bind(new InetSocketAddress(PORT));
			listenChannel.register(selector, SelectionKey.OP_ACCEPT);
			while(true){
				if(selector.select(TIMEOUT)==0){
					System.out.print("");
					continue;
				}
				Iterator<SelectionKey> iterator=selector.selectedKeys().iterator();
				while(iterator.hasNext()){
					SelectionKey key=iterator.next();
					iterator.remove();
					
					if(key.isAcceptable()){
						SocketChannel channel=listenChannel.accept();
						channel.configureBlocking(false);
						SelectionKey connKey=channel.register(selector, SelectionKey.OP_READ);
						NIOServerConnection conn=new NIOServerConnection(connKey);
						connKey.attach(conn);
					}
					
					if(key.isReadable()){
						NIOServerConnection conn=(NIOServerConnection)key.attachment();
						conn.handleRead();
					}
					
					if(key.isWritable()){
						NIOServerConnection conne=(NIOServerConnection)key.attachment();
						conne.handleWrite();
					}
				}
			}
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}

	}

}
