package org.phalanxdev.python;

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertThrows;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class ArrowUtilsTest { // Replace with your actual test class name

    @Mock
    private IRowMeta rowMeta;

    @Mock
    private IValueMeta valueMeta1;

    @Mock
    private IValueMeta valueMeta2;

    @Mock
    private VectorSchemaRoot rootMock;

    @Mock
    private FieldVector fieldVectorMock1;

    @Mock
    private FieldVector fieldVectorMock2;

    @Mock
    private Field dummyField1;

    @Mock
    private Field dummyField2;

    @Test
    void rowsToArrow_ConvertsRowsToArrowIPCStreamSuccessfully() throws Exception {
        // Arrange: 2 columns, 2 rows of data
        List<Object[]> rows = Arrays.asList(
                new Object[]{"Row1Col1", "Row1Col2"},
                new Object[]{"Row2Col1", "Row2Col2"}
        );

        when(rowMeta.size()).thenReturn(2);
        when(rowMeta.getValueMeta(0)).thenReturn(valueMeta1);
        when(rowMeta.getValueMeta(1)).thenReturn(valueMeta2);

        // We mock the ArrowUtils class itself to intercept its internal static calls, 
        // while instructing it to call the real method for rowsToArrow.
        try (MockedStatic<ArrowUtils> mockedUtils = mockStatic(ArrowUtils.class);
             MockedStatic<VectorSchemaRoot> mockedRootStatic = mockStatic(VectorSchemaRoot.class);
             MockedConstruction<ArrowStreamWriter> mockedWriter = mockConstruction(ArrowStreamWriter.class)) {

            // Call the real method we are testing
            mockedUtils.when(() -> ArrowUtils.rowsToArrow(eq(rowMeta), eq(rows))).thenCallRealMethod();

            // Mock the internal static field/value mapping methods
            mockedUtils.when(() -> ArrowUtils.createArrowField(valueMeta1)).thenReturn(dummyField1);
            mockedUtils.when(() -> ArrowUtils.createArrowField(valueMeta2)).thenReturn(dummyField2);

            // Mock VectorSchemaRoot instantiation and field vectors
            mockedRootStatic.when(() -> VectorSchemaRoot.create(any(Schema.class), any())).thenReturn(rootMock);
            when(rootMock.getVector(0)).thenReturn(fieldVectorMock1);
            when(rootMock.getVector(1)).thenReturn(fieldVectorMock2);

            // Act
            byte[] result = ArrowUtils.rowsToArrow(rowMeta, rows);

            // Assert
            assertNotNull(result, "Resulting byte array should not be null");

            // Verify Arrow Lifecycle Calls
            verify(rootMock).allocateNew();
            verify(rootMock).setRowCount(2);
            verify(rootMock).close(); // Ensures try-with-resources cleaned up root

            // Verify data population was routed correctly
            mockedUtils.verify(() -> ArrowUtils.setVectorValue(fieldVectorMock1, 0, "Row1Col1", valueMeta1));
            mockedUtils.verify(() -> ArrowUtils.setVectorValue(fieldVectorMock2, 0, "Row1Col2", valueMeta2));
            mockedUtils.verify(() -> ArrowUtils.setVectorValue(fieldVectorMock1, 1, "Row2Col1", valueMeta1));
            mockedUtils.verify(() -> ArrowUtils.setVectorValue(fieldVectorMock2, 1, "Row2Col2", valueMeta2));

            // Verify Writer Lifecycle Calls
            assertEquals(1, mockedWriter.constructed().size(), "One ArrowStreamWriter should have been created");
            ArrowStreamWriter writerMock = mockedWriter.constructed().get(0);
            verify(writerMock).start();
            verify(writerMock).writeBatch();
            verify(writerMock).end();
            verify(writerMock).close();
        }
    }

    @Test
    void rowsToArrow_ThrowsHopException_WhenIOExceptionOccurs() throws Exception {
        // Arrange: 1 column, 1 row
    	List<Object[]> rows = Collections.singletonList(new Object[]{"Data"});

        when(rowMeta.size()).thenReturn(1);
        when(rowMeta.getValueMeta(0)).thenReturn(valueMeta1);

        try (MockedStatic<ArrowUtils> mockedUtils = mockStatic(ArrowUtils.class);
             MockedStatic<VectorSchemaRoot> mockedRootStatic = mockStatic(VectorSchemaRoot.class);
             // Intercept ArrowStreamWriter creation and force an IOException when writer.start() is called
             MockedConstruction<ArrowStreamWriter> mockedWriter = mockConstruction(ArrowStreamWriter.class,
                     (mock, context) -> doThrow(new IOException("Simulated IO Stream Failure")).when(mock).start())) {

            mockedUtils.when(() -> ArrowUtils.rowsToArrow(eq(rowMeta), eq(rows))).thenCallRealMethod();
            mockedUtils.when(() -> ArrowUtils.createArrowField(valueMeta1)).thenReturn(dummyField1);

            mockedRootStatic.when(() -> VectorSchemaRoot.create(any(Schema.class), any())).thenReturn(rootMock);
            when(rootMock.getVector(0)).thenReturn(fieldVectorMock1);

            // Act & Assert
            HopException exception = assertThrows(HopException.class, () -> {
                ArrowUtils.rowsToArrow(rowMeta, rows);
            });

            // Verify the exception was properly caught and wrapped
            assertTrue(exception.getMessage().contains("Error converting rows to Arrow format"));
            assertInstanceOf(IOException.class, exception.getCause());
            assertEquals("Simulated IO Stream Failure", exception.getCause().getMessage());
        }
    }
}