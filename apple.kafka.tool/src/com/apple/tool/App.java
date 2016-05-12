package com.apple.tool;

import java.util.prefs.Preferences;

import org.eclipse.swt.SWT;
import org.eclipse.swt.events.ModifyEvent;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.ShellAdapter;
import org.eclipse.swt.events.ShellEvent;
import org.eclipse.swt.graphics.Point;
import org.eclipse.swt.layout.FillLayout;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.ProgressBar;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TabFolder;
import org.eclipse.swt.widgets.TabItem;
import org.eclipse.swt.widgets.Text;
import org.eclipse.wb.swt.SWTResourceManager;

public class App {

	protected Shell shlKafkaTool;
	private Text textConnProd;
	private Text textMsg;
	private Text textTopic;
	private Text textConnConsumer;
	private Text textTopicConsumer;
	private Text textMsgReceive;

	private KafkaConsumer consumer;
	private Preferences prefs;
	private Text textGroupId;

	/**
	 * Launch the application.
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		try {
			App window = new App();
			window.open();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * Open the window.
	 */
	public void open() {
		Display display = Display.getDefault();
		createContents();
		init();
		shlKafkaTool.open();
		shlKafkaTool.layout();
		while (!shlKafkaTool.isDisposed()) {
			if (!display.readAndDispatch()) {
				display.sleep();
			}
		}
	}

	private void init() {
		prefs = Preferences.userRoot().node(this.getClass().getName());
		try {
			textConnProd.setText(prefs.get("prod.conn", ""));
			textTopic.setText(prefs.get("prod.topic", ""));
			textConnConsumer.setText(prefs.get("consumer.conn", ""));
			textTopicConsumer.setText(prefs.get("consumer.topic", ""));
			textGroupId.setText(prefs.get("group.id", ""));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * Create contents of the window.
	 */
	protected void createContents() {
		shlKafkaTool = new Shell();
		shlKafkaTool.addShellListener(new ShellAdapter() {
			@Override
			public void shellClosed(ShellEvent e) {
				if (consumer != null) {
					consumer.stop();
				}
				try {
					prefs.put("prod.conn", textConnProd.getText());
					prefs.put("consumer.conn", textConnConsumer.getText());
					prefs.put("prod.topic", textTopic.getText());
					prefs.put("consumer.topic", textTopicConsumer.getText());
					prefs.put("group.id", textGroupId.getText());
				} catch (Exception e1) {
					e1.printStackTrace();
				}
			}
		});
		shlKafkaTool.setMinimumSize(new Point(800, 600));
		shlKafkaTool.setSize(467, 300);
		shlKafkaTool.setText("Kafka Tool beta 0.1 -- David Chen");
		shlKafkaTool.setLayout(new FillLayout(SWT.HORIZONTAL));

		TabFolder tabFolder = new TabFolder(shlKafkaTool, SWT.NONE);
		tabFolder.setBackground(SWTResourceManager.getColor(SWT.COLOR_WHITE));

		TabItem tbtmProducer = new TabItem(tabFolder, SWT.NONE);
		tbtmProducer.setText("producer");

		Composite composite_prod = new Composite(tabFolder, SWT.NONE);
		composite_prod.setBackground(SWTResourceManager.getColor(SWT.COLOR_WHITE));
		tbtmProducer.setControl(composite_prod);
		composite_prod.setLayout(new FormLayout());

		Label lblConnection = new Label(composite_prod, SWT.NONE);
		FormData fd_lblConnection = new FormData();
		fd_lblConnection.left = new FormAttachment(0, 10);
		fd_lblConnection.top = new FormAttachment(0, 10);
		lblConnection.setLayoutData(fd_lblConnection);
		lblConnection.setText("connection:");

		textConnProd = new Text(composite_prod, SWT.BORDER);
		fd_lblConnection.right = new FormAttachment(textConnProd, -6);
		FormData fd_textConnProd = new FormData();
		fd_textConnProd.right = new FormAttachment(100, -10);
		fd_textConnProd.left = new FormAttachment(0, 110);
		fd_textConnProd.top = new FormAttachment(0, 10);
		textConnProd.setLayoutData(fd_textConnProd);

		final Label lblSever = new Label(composite_prod, SWT.BORDER | SWT.WRAP);
		lblSever.setFont(SWTResourceManager.getFont(".Helvetica Neue DeskInterface", 10, SWT.NORMAL));
		lblSever.setForeground(SWTResourceManager.getColor(SWT.COLOR_DARK_GRAY));
		FormData fd_lblSever = new FormData();
		fd_lblSever.top = new FormAttachment(textConnProd, 32);
		fd_lblSever.right = new FormAttachment(0, 690);
		fd_lblSever.left = new FormAttachment(0, 10);
		lblSever.setLayoutData(fd_lblSever);

		Button btnSend = new Button(composite_prod, SWT.NONE);
		btnSend.addSelectionListener(new SelectionAdapter() {
			@Override
			public void widgetSelected(SelectionEvent e) {
				lblSever.setText("sending ...");
				try {
					KafkaMessageProducer.send(textConnProd.getText(), textTopic.getText(), textMsg.getText(), "0");
					lblSever.setText("done ...");
				} catch (Exception ex) {
					lblSever.setText(ex.getMessage());
				}
			}
		});
		FormData fd_btnSend = new FormData();
		fd_btnSend.top = new FormAttachment(textConnProd, 24);
		fd_btnSend.right = new FormAttachment(textConnProd, 0, SWT.RIGHT);
		btnSend.setLayoutData(fd_btnSend);
		btnSend.setText("send");

		textMsg = new Text(composite_prod, SWT.BORDER | SWT.V_SCROLL | SWT.MULTI);
		textMsg.addModifyListener(new ModifyListener() {
			public void modifyText(ModifyEvent e) {
				lblSever.setText("");
			}
		});
		fd_lblSever.bottom = new FormAttachment(textMsg, -14);
		FormData fd_textMsg = new FormData();
		fd_textMsg.left = new FormAttachment(0, 10);
		fd_textMsg.right = new FormAttachment(100, -10);
		fd_textMsg.top = new FormAttachment(0, 89);
		fd_textMsg.bottom = new FormAttachment(100, -2);
		textMsg.setLayoutData(fd_textMsg);

		Label lblTopic = new Label(composite_prod, SWT.NONE);
		FormData fd_lblTopic = new FormData();
		fd_lblTopic.top = new FormAttachment(lblConnection, 8);
		fd_lblTopic.left = new FormAttachment(0, 10);
		lblTopic.setLayoutData(fd_lblTopic);
		lblTopic.setText("topic:");

		textTopic = new Text(composite_prod, SWT.BORDER);
		FormData fd_textTopic = new FormData();
		fd_textTopic.right = new FormAttachment(textConnProd, 0, SWT.RIGHT);
		fd_textTopic.top = new FormAttachment(textConnProd, 2);
		fd_textTopic.left = new FormAttachment(textConnProd, 0, SWT.LEFT);
		textTopic.setLayoutData(fd_textTopic);

		TabItem tbtmConsumer = new TabItem(tabFolder, SWT.NONE);
		tbtmConsumer.setText("consumer");

		Composite composite_consumer = new Composite(tabFolder, SWT.NONE);
		composite_consumer.setBackground(SWTResourceManager.getColor(SWT.COLOR_WHITE));
		tbtmConsumer.setControl(composite_consumer);
		composite_consumer.setLayout(new FormLayout());

		Label lblConnetion = new Label(composite_consumer, SWT.NONE);
		FormData fd_lblConnetion = new FormData();
		fd_lblConnetion.top = new FormAttachment(0, 10);
		fd_lblConnetion.left = new FormAttachment(0, 10);
		lblConnetion.setLayoutData(fd_lblConnetion);
		lblConnetion.setText("connection: ");

		textConnConsumer = new Text(composite_consumer, SWT.BORDER);
		FormData fd_textConnConsumer = new FormData();
		fd_textConnConsumer.left = new FormAttachment(0, 110);
		fd_textConnConsumer.right = new FormAttachment(100, -10);
		fd_textConnConsumer.top = new FormAttachment(0, 10);
		textConnConsumer.setLayoutData(fd_textConnConsumer);

		Label lblTopicConsumer = new Label(composite_consumer, SWT.NONE);
		FormData fd_lblTopicConsumer = new FormData();
		fd_lblTopicConsumer.top = new FormAttachment(lblConnetion, 8);
		fd_lblTopicConsumer.left = new FormAttachment(0, 10);
		lblTopicConsumer.setLayoutData(fd_lblTopicConsumer);
		lblTopicConsumer.setText("topic:");

		textTopicConsumer = new Text(composite_consumer, SWT.BORDER);
		FormData fd_textTopicConsumer = new FormData();
		fd_textTopicConsumer.top = new FormAttachment(textConnConsumer, 2);
		fd_textTopicConsumer.left = new FormAttachment(textConnConsumer, 0, SWT.LEFT);
		fd_textTopicConsumer.right = new FormAttachment(textConnConsumer, 0, SWT.RIGHT);
		textTopicConsumer.setLayoutData(fd_textTopicConsumer);

		final ProgressBar progressBar = new ProgressBar(composite_consumer, SWT.BORDER);
		progressBar.setBackground(SWTResourceManager.getColor(SWT.COLOR_DARK_GREEN));
		FormData fd_progressBar = new FormData();
		fd_progressBar.left = new FormAttachment(lblConnetion, 0, SWT.LEFT);
		fd_progressBar.right = new FormAttachment(100, -72);
		progressBar.setLayoutData(fd_progressBar);

		final Button btnStartConsumer = new Button(composite_consumer, SWT.NONE);
		btnStartConsumer.addSelectionListener(new SelectionAdapter() {

			@Override
			public void widgetSelected(SelectionEvent e) {
				if (btnStartConsumer.getText().equals("start")) {

					progressBar.setSelection(30);
					btnStartConsumer.setEnabled(false);
					consumer = new KafkaConsumer(textConnConsumer.getText(), textTopicConsumer.getText(),
							textGroupId.getText(), textMsgReceive, progressBar, btnStartConsumer);
					Thread thread = new Thread(consumer);
					thread.start();

				} else {
					consumer.stop();
				}
			}
		});
		FormData fd_btnStartConsumer = new FormData();
		fd_btnStartConsumer.top = new FormAttachment(progressBar, -6, SWT.TOP);
		fd_btnStartConsumer.right = new FormAttachment(textConnConsumer, 0, SWT.RIGHT);
		btnStartConsumer.setLayoutData(fd_btnStartConsumer);
		btnStartConsumer.setText("start");

		textMsgReceive = new Text(composite_consumer, SWT.BORDER | SWT.WRAP | SWT.V_SCROLL | SWT.MULTI);
		FormData fd_textMsgReceive = new FormData();
		fd_textMsgReceive.top = new FormAttachment(btnStartConsumer, 2);
		fd_textMsgReceive.bottom = new FormAttachment(100, -2);
		fd_textMsgReceive.right = new FormAttachment(textConnConsumer, 0, SWT.RIGHT);
		fd_textMsgReceive.left = new FormAttachment(0, 10);
		textMsgReceive.setLayoutData(fd_textMsgReceive);

		Label lblGroupId = new Label(composite_consumer, SWT.NONE);
		FormData fd_lblGroupId = new FormData();
		fd_lblGroupId.top = new FormAttachment(lblTopicConsumer, 6);
		fd_lblGroupId.left = new FormAttachment(lblConnetion, 0, SWT.LEFT);
		lblGroupId.setLayoutData(fd_lblGroupId);
		lblGroupId.setText("group id:");

		textGroupId = new Text(composite_consumer, SWT.BORDER);
		fd_progressBar.top = new FormAttachment(textGroupId, 11);
		FormData fd_textGroupId = new FormData();
		fd_textGroupId.right = new FormAttachment(textConnConsumer, 0, SWT.RIGHT);
		fd_textGroupId.top = new FormAttachment(textTopicConsumer, 2);
		fd_textGroupId.left = new FormAttachment(textConnConsumer, 0, SWT.LEFT);
		textGroupId.setLayoutData(fd_textGroupId);

	}
}
